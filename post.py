from fastapi import FastAPI, Query, HTTPException
from googleapiclient.discovery import build
import pymongo
import psycopg2
from psycopg2 import sql

app = FastAPI()

API_KEY = 'AIzaSyDGvNNklTTV0WovK3YsN-BoMNgYcgAwzrg'
youtube = build('youtube', 'v3', developerKey=API_KEY)

def youtube_search(query, max_results=5):
    request = youtube.search().list(
        q=query,
        part='snippet',
        type='video',
        maxResults=max_results,
    )
    response = request.execute()
    return response['items']

def get_videos_from_channel(channel_id):
    request = youtube.search().list(
        part='snippet',
        channelId=channel_id,
        maxResults=50,
        order='date'
    )
    response = request.execute()
    return response['items']

def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,statistics",
        id=channel_id
    )
    response = request.execute()
    return response['items'][0]

def get_dislikes(video_id):
    request = youtube.videos().list(
        part='statistics',
        id=video_id
    )
    response = request.execute()
    if 'items' in response and response['items']:
        return int(response['items'][0]['statistics'].get('dislikeCount', 0))
    else:
        return 0

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="aakarshaindukuri",
    user="myuser",
    password="mypassword",
    host="localhost",
    port="5432"
)

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["youtube_search_results"]
collection = db["likes_comments"]

@app.get('/api/youtube/search')
async def search_youtube(q: str = Query(None, title="Query", description="Query string"),
                         maxResults: int = Query(10, title="Max Results", description="Maximum number of results")):
    if not q:
        raise HTTPException(status_code=400, detail='Query parameter "q" is required')

    with conn.cursor() as cursor:
        cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'videos')")
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            raise HTTPException(status_code=500, detail='The "videos" table does not exist. Please create the table first.')

        videos = youtube_search(q, maxResults)
        videos = videos[:maxResults]

        inserted_data = []
        for video in videos:
            title = video['snippet']['title']
            description = video['snippet']['description']
            channel_id = video['snippet']['channelId']

            cursor.execute(sql.SQL("INSERT INTO videos (query, title, description, channel_id) VALUES (%s, %s, %s, %s) RETURNING id"),
                           (q, title, description, channel_id))
            inserted_id = cursor.fetchone()[0]
            inserted_data.append({
                "id": inserted_id,
                "query": q,
                "title": title,
                "description": description,
                "channel_id": channel_id
            })

    thumbnails_records = []
    for video in videos:
        thumbnails_records.append({
            "query": q,
            "url": video['snippet']['thumbnails']['default']['url']
        })

    thumbnails_collection = db["thumbnails"]
    thumbnails_collection.insert_one({"query": q, "results": thumbnails_records})

    conn.commit()

    return {
        'message': 'Data inserted into the "videos" table and thumbnails inserted into MongoDB successfully.',
        'inserted_data': inserted_data,
        'thumbnails_inserted': thumbnails_records
    }

@app.get('/api/youtube/channel')
async def get_channel(channel_id: str = Query(..., title="Channel ID", description="YouTube Channel ID")):
    channel_info = get_channel_info(channel_id)
    snippet = channel_info['snippet']
    statistics = channel_info.get('statistics', {})

    channel_name = snippet['title']
    subscriber_count = int(statistics.get('subscriberCount', 0))
    video_count = int(statistics.get('videoCount', 0))

    with conn.cursor() as cursor:
        cursor.execute("CREATE TABLE IF NOT EXISTS channels (id SERIAL PRIMARY KEY, channel_id VARCHAR(255), channel_name VARCHAR(255), subscriber_count INT, video_count INT)")
        cursor.execute(sql.SQL("INSERT INTO channels (channel_id, channel_name, subscriber_count, video_count) VALUES (%s, %s, %s, %s) RETURNING id"),
                       (channel_id, channel_name, subscriber_count, video_count))
        _ = cursor.fetchone()[0]

    videos = get_videos_from_channel(channel_id)

    collection = db["channel_data"]

    channel_data = {
        "channel_id": channel_id,
        "channel_name": channel_name,
        "number_of_subscribers": subscriber_count,
        "number_of_videos": video_count,
        "thumbnails": snippet['thumbnails']
    }
    collection.insert_one(channel_data)

    for video in videos:
        if 'id' in video and 'videoId' in video['id']:
            video_id = video['id']['videoId']
            title = video['snippet']['title']
            description = video['snippet']['description']
            channel_title = video['snippet']['channelTitle']
            channel_id = video['snippet']['channelId']
            publish_time = video['snippet']['publishedAt']
            thumbnails = video['snippet']['thumbnails']

            dislikes = get_dislikes(video_id)

            video_response = youtube.videos().list(
                part='snippet,statistics',
                id=video_id
            ).execute()

            if 'items' in video_response and len(video_response['items']) > 0:
                video_info = video_response['items'][0]
                statistics = video_info.get('statistics', {})
                likes = int(statistics.get('likeCount', 0))
                views = int(statistics.get('viewCount', 0))
                comments = int(statistics.get('commentCount', 0))

                video_data = {
                    "video_id": video_id,
                    "title": title,
                    "description": description,
                    "channel_id": channel_id,
                    "channel_title": channel_title,
                    "publish_time": publish_time,
                    "thumbnails": thumbnails,
                    "likes": likes,
                    "dislikes": dislikes,
                    "views": views,
                    "comments": comments
                }

                collection.insert_one(video_data)

    return {
        "channel_id": channel_id,
        "channel_name": channel_name,
        "number_of_subscribers": subscriber_count,
        "number_of_videos": video_count,
        "thumbnails": snippet['thumbnails'],
        "videos": videos
    }

@app.get("/video/{video_id}")
async def get_video_info(video_id: str):
    try:
        video_response = youtube.videos().list(
            part='snippet,statistics',
            id=video_id
        ).execute()

        if 'items' in video_response and len(video_response['items']) > 0:
            video_info = video_response['items'][0]
            snippet = video_info['snippet']
            statistics = video_info['statistics']

            title = snippet['title']
            description = snippet.get('description', '')
            channel_title = snippet['channelTitle']
            publish_time = snippet['publishedAt']
            likes = int(statistics['likeCount'])
            dislikes = get_dislikes(video_id)
            comments = int(statistics['commentCount'])

            data = {
                "video_id": video_id,
                "title": title,
                "description": description,
                "channel_title": channel_title,
                "publish_time": publish_time,
                "likes": likes,
                "dislikes": dislikes,
                "comments": comments
            }

            collection.insert_one(data)

            return {
                "title": title,
                "description": description,
                "channel_title": channel_title,
                "publish_time": publish_time,
                "likes": likes,
                "dislikes": dislikes,
                "comments": comments
            }
        else:
            raise HTTPException(status_code=404, detail="Video not found")
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
