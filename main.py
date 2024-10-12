from fasthtml.common import *
from pymongo import MongoClient
from dotenv import load_dotenv
from bson.objectid import ObjectId
import os

load_dotenv()

user = os.environ.get('MONGODB_USER')
psw = os.environ.get('MONGODB_PSW')
mongo_uri = os.environ.get('MONGODB_URI')
db = MongoClient(f'mongodb://{user}:{psw}@{mongo_uri}/mjd?ssl=true', ssl=True, tlsAllowInvalidCertificates=True).mjd

app, rt = fast_app()

# Home Page
@rt("/")
def home():
    courses = db.disciplinas.find({"turma": "MJD003"}).sort("tri", -1)
    courses_by_tri = {}
    for course in courses:
        tri = course['tri']
        if tri not in courses_by_tri:
            courses_by_tri[tri] = []
        courses_by_tri[tri].append(course)
    print(courses_by_tri)
    return Titled("Master em Jornalismo de Dados, Automação e Data Storytelling",
                  P("Clique no nome da disciplina para acessar as gravações das aulas"),
                  *[Card(
                      H3(f"{tri}º trimestre"),
                      Ul(*[Li(A(course["nome"], href=f"/courses/{course['zoom_id']}")) for course in courses_by_tri[tri]])
                  ) for tri in sorted(set(courses_by_tri.keys()), reverse=True)])



@rt("/courses/{course_id}")
def course_page(course_id: int):
    course = db.disciplinas.find_one({"zoom_id": course_id})
    classes = db.gravacoes.find({"meeting_id": course["zoom_id"]})
    return Titled(f"{course['nome']}", 
                  *[class_card(recording, i) for i, recording in enumerate(classes, 1)],
                  A(href="/logout")("Logout"))


@rt("/expand/{recording_id}")
def get_summary(recording_id: str):
    recording = db.gravacoes.find_one({"_id": ObjectId(recording_id)})
    return P(f"({recording['data_str']}) {recording['ai_summary']['summary']}"),\
           Ul(*[Li(f"{block['start']} - {block['block']}") for block in recording["ai_summary"]['blocks']])

def class_card(recording: dict, i: int):
    return Card(
        H3(f"{i}. {recording['ai_summary']['title']}"),
        P(
            A(href=recording['download_url'], target="_blank", title="Download recording")
          (Img(src="/static/img/download_button.svg", alt="download icon", width="32", height="32", cls="w-4 h-4"),
           ),
           Div("➕ Detalhes", hx_get=f"/expand/{recording['_id']}", hx_target=f"#summary-{recording['_id']}", hx_swap="innerHTML")
        ),
        Div(id=f"summary-{recording['_id']}")
    )



# Serve the app
serve()
