from fastapi import FastAPI
import router as router
import asyncio

#create an api object, a new fastapi
app = FastAPI()

#endpoint
@app.get('/')
async def Home():
    return "hello world"

app.include_router(router.route)
asyncio.create_task(router.send())

#@app.get("/")
#def home():
#    return {"Data": "Testing"}

#@app.get("/about")
#def about():
#    return {"Data": "About"}

#visualiser les stations les plus fréquentées
#récupère les données grâce à kafka
#données dynamiques
#retourne un dictionnaire des infos désirées