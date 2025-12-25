from fastapi import FastAPI, Response, status,HTTPException
from fastapi.params import Body
from pydantic import BaseModel
app = FastAPI()
from random import randrange
# startup uvicorn main:app

# "DATABASE"
class Post(BaseModel):
    title:str
    content:str
    id:int = randrange(0, 99999999)

all_posts = [Post(title="Title_0",content="Content of Title_0",id=0),
             Post(title="Title_1",content="Content of Title_1",id=1)]
# Helper FUNCS
def search_data_by_id(id):
    for dd in all_posts:
        print(f'{dd.title}: Id {dd.id} | Searched Id: {id}')

        if dd.id == id:
            return dd
    return None
def find_index(id)-> int:
    for ii,pp in enumerate(all_posts):
        if pp.id == id:
            return ii
def get_latest_data() -> dict:
    return all_posts[-1]

# API
@app.get("/",
         status_code=status.HTTP_200_OK)
def get():
    return {"message": "Hello"}

@app.get("/posts",
         status_code=status.HTTP_200_OK)
def get_all():
    """
    Returns all data from Database
    """
    return {"data": all_posts}

@app.get("/posts/latest",
         status_code=status.HTTP_200_OK)
def get_latest():
    """
    Returns dataset of the latest Database-Entry
    """
    latest_post = get_latest_data()
    return {'data':latest_post}

@app.get("posts/{id}/",
         status_code=status.HTTP_200_OK)
def get_distinct(id):
    """
    Returns distinct dataset based on id
    Returns 404 if id does not exist
    """
    print(f"{id}, type {type(id)}")
    post = search_data_by_id(int(id))
    if post is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail= f"Post with Id '{id} was not found.'")
    return {'data': post}

@app.post("/posts/create",
          status_code=status.HTTP_201_CREATED)
def create_posts(post: Post):
    """
    Create database-entry

    :param post: Beschreibung
    :type post: Post
    """
    all_posts.append(post)
    return {"data": post}

@app.delete("/posts/del{id}",
            status_code=status.HTTP_204_NO_CONTENT)
def delete_posts(id):
    ii = find_index(id)
    deleted_post = all_posts.pop(ii)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
