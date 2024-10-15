import re
import time
from operator import itemgetter
from unidecode import unidecode
from slack_sdk import WebClient
import requests
from pymongo import MongoClient
import base64
from pyzoom import ZoomClient
import boto3
import arrow
from dotenv import load_dotenv
import os
from pydantic import BaseModel, Field
import ell
from typing import List

load_dotenv()


## AI STUFF
class Block(BaseModel):
    block: str = Field(description="Os temas e conceitos que foram abordados neste trecho da aula, em bullet points. Seja específico sobre todos os conceitos e conteúdos.")
    start: str = Field(description="O tempo de início deste trecho no formato HH:MM:SS.MS")


class Summary(BaseModel):
    summary: str = Field(description="Um resumo em um parágrafo sobre o que foi abordado na aula")
    blocks: List[Block] = Field(description="Uma lista de trechos da aula, cada um com os assuntos abordados, e o tempo correspondente no vídeo. O foco deve ser exclusivamente o conteúdo da aula, e não na descrição do que aconteceu.")
    
    
class FinalSummary(BaseModel):
    summary: str = Field(description="Um resumo em um parágrafo sobre o que foi abordado na aula")
    title: str = Field(description="Um título para a aula, com breve lista de conceitos mais importantesentre parênteses")

    

@ell.complex(model="gpt-4o", response_format=FinalSummary)
def fix_class_summary(blocks: List[Block], disciplina: str) -> FinalSummary:
    """Você é um professor assistente que recebe um resumo estruturado de uma aula e corrige possíveis erros de formatação, sem alterar o conteúdo."""
    return f"Leia a seguinte lista de trechos de uma aula da disciplina {disciplina}. Crie um título para a aula, dentro do contexto da disciplina, e um resumo em um parágrafo sobre o que foi abordado: {blocks}"


@ell.complex(model="gpt-4o-mini", response_format=Summary)
def generate_class_summary(transcription: str) -> Summary:
    """Você é um professor assistente que recebe a transcrição de uma aula e gera um resumo estruturado com o conteúdo abordado."""
    return f"Gere um resumo da seguinte aula: {transcription}"


def parse_summary(summary):
    data = {}
    parsed = summary.content[0].parsed
    data['summary'] = parsed.summary
    data['blocks'] = []
    for block in parsed.blocks:
        data['blocks'].append({
            'block': block.block,
            'start': block.start
        })
    return data


## DATABASE AND ZOOM STUFF
def initiate_mongo_db():
    user = os.environ.get('MONGODB_USER')
    psw = os.environ.get('MONGODB_PSW')
    mongo_uri = os.environ.get('MONGODB_URI')
    uri = f'mongodb://{user}:{psw}@{mongo_uri}/mjd?ssl=true'
    db = MongoClient(uri, ssl=True, tlsAllowInvalidCertificates=True).mjd
    return db


def initiate_zoom_app():
    db = initiate_mongo_db()
    stored_refresh_token = db.utils.find_one({"function": "zoom_refresher"})['token']
    try:
        zoom_client_id = os.environ.get("ZOOM_APP_CLIENT_ID")
        zoom_client_secret = os.environ.get("ZOOM_APP_CLIENT_SECRET")

        if zoom_client_secret is None or zoom_client_id is None:
            raise Exception("Zoom client ID and secret not found")

        # Prepare data for token refresh request
        token_url = "https://zoom.us/oauth/token"
        token_data = {
            "grant_type": "refresh_token",
            "refresh_token": stored_refresh_token
        }
        token_headers = {
            "Authorization": f"Basic {base64.b64encode(f'{zoom_client_id}:{zoom_client_secret}'.encode()).decode()}"
        }

        # Make the token refresh request
        token_r = requests.post(token_url, data=token_data, headers=token_headers)
        token_r_json = token_r.json()

        # Extract the tokens from the response
        new_token = token_r_json['access_token']
        new_refresh_token = token_r_json['refresh_token']
        db.utils.update_one({"function": "zoom_refresher"}, {"$set": {"token": new_refresh_token}})

        client = ZoomClient(access_token=new_token)  # type: ignore
        return client, new_token, new_refresh_token

    except Exception as e:
        print(f"An error occurred while refreshing Zoom tokens: {e}")
        import traceback
        traceback.print_exc()



def get_meeting_info(client, meeting_id, token):
    s = client.raw.get(f"/meetings/{meeting_id}/recordings")
    dados = s.json()
    obj = {}
    obj['disciplina'] = dados['topic']
    obj['data'] = arrow.get(dados['start_time']).datetime
    obj['data_str'] = arrow.get(dados['start_time']).format("DD/MM/YY")
    files = sorted(dados['recording_files'], key=itemgetter('file_size'), reverse=True)
    obj['video_url'] = files[0]['play_url']
    obj['audio_url'] = [x for x in files if x['recording_type'] == 'audio_only'][0]['play_url']
    transcricao_list = [x for x in files if x['recording_type'] == 'audio_transcript']
    if len(transcricao_list) == 0:
        obj['transcription'] = None
    else:
        obj['transcription'] = f"{transcricao_list[0]['download_url']}?access_token={token}"
    obj['psw'] = dados['password']
    obj['meeting_id'] = meeting_id
    obj['recording_id'] = files[0]['id']
    obj['download_url'] = files[0]['download_url'] + f"?access_token={token}"
    return obj


def msg_nova_gravacao(json, slack_client, url_presenca, channel="general"):
    t = f":red_circle: A gravação da última aula da disciplina *{json['disciplina']}* já está disponível!\n"
    t += f"Clique <{json['video_url']}|aqui> para acessar o vídeo.\n"
    t += f"Para baixar o arquivo .mp4 clique <{json['download_url']}|aqui>.\n"
    block = [{"type": "section", "text": {"type": "mrkdwn", "text": t}}]
    slack_client.chat_postMessage(channel=channel, text="", blocks=block)
    time.sleep(2)
    p = f"Lista de presença: <{url_presenca}|aqui>"
    slack_client.chat_postMessage(channel=channel, text=p)
    print("Message sent")


def msg_nova_transcricao(markdown, slack_client, channel="general"):
    if not markdown:
        markdown = "A transcrição da última aula ainda não está disponível. Não consegui fazer um resumo. :disappointed:"
    block = [{"type": "section", "text": {"type": "mrkdwn", "text": markdown}}]
    slack_client.chat_postMessage(channel=channel, text="", blocks=block)
    print("Transcrição enviada")


def get_meeting_info(client, meeting_id, token):
    s = client.raw.get(f"/meetings/{meeting_id}/recordings")
    dados = s.json()
    obj = {}
    obj['disciplina'] = dados['topic']
    obj['data'] = arrow.get(dados['start_time']).datetime
    obj['data_str'] = arrow.get(dados['start_time']).format("DD/MM/YY")
    files = sorted(dados['recording_files'],
                   key=itemgetter('file_size'), reverse=True)
    obj['video_url'] = files[0]['play_url']
    obj['audio_url'] = [x for x in files if x['recording_type']
                        == 'audio_only'][0]['play_url']
    transcricao_list = [
        x for x in files if x['recording_type'] == 'audio_transcript']
    if len(transcricao_list) == 0:
        obj['transcription'] = None
    else:
        obj['transcription'] = f"{transcricao_list[0]['download_url']}?access_token={token}"
    obj['psw'] = dados['password']
    obj['meeting_id'] = meeting_id
    obj['recording_id'] = files[0]['id']
    obj['download_url'] = files[0]['download_url'] + f"?access_token={token}"
    return obj


def cria_lista_presenca(disciplina, data, presenca_total, presenca_parcial):
    data_str = data.strftime('%d/%m/%Y')
    safe_name = unidecode(disciplina.lower().replace(" ", "-"))
    filename = f'{safe_name}-{data_str.replace("/", "-")}.txt'
    # Open the text file in write mode
    with open(filename, 'w') as f:
        f.write('Master em Jornalismo de Dados - LISTA DE PRESENÇA\n')
        f.write(f'Disciplina: {disciplina} - {data_str}')
        f.write('\n')
        f.write('\n')
        f.write('Presença:\n')

        for student in sorted(presenca_total):
            f.write(f'{student}\n')

        f.write('\n')
        f.write('\n')
        f.write('Presença em parte da aula:\n')
        for student in sorted(presenca_parcial):
            f.write(f'{student}\n')

        f.write('\n')
        f.write('\n')
        f.write('* Se você esteve na aula mas não vê o seu nome aqui, entre em contato com a gente. E lembre-se de usar o seu nome corretamente nos ajustes do Zoom.')

    url = send_file_to_s3(filename)
    os.remove(filename)
    return url


def lista_presenca(zoom_client, meeting_id, tempo_de_aula=3*60*60):
    s = zoom_client.raw.get(
        f'/past_meetings/{meeting_id}/participants?page_size=200')
    participantes = set([x['name'] for x in s.json()['participants']])
    presenca = {x: 0 for x in participantes}
    for p in s.json()['participants']:
        presenca[p['name']] += p['duration']
    completion = {}
    for student, time in presenca.items():
        percent = (time / tempo_de_aula) * 100
        completion[student] = round(percent, 2)
    presenca_total = []
    presenca_parcial = []
    for i in list(completion.items()):
        if i[1] > 60:
            presenca_total.append(i[0])
        else:
            presenca_parcial.append(i[0])
    return {"presenca_total": presenca_total, "presenca_parcial": presenca_parcial}


def send_file_to_s3(filename):
    s3 = boto3.client('s3', aws_access_key_id=os.environ.get("AWS_KEY"),
                      aws_secret_access_key=os.environ.get("AWS_SECRET"))
    bucket_name = 'mjd-insper'
    url = f'https://mjd-insper.s3.sa-east-1.amazonaws.com/{filename}'
    s3.upload_file(filename, bucket_name, filename,
                   ExtraArgs={'ACL': 'public-read'})
    return url


def send_large_file_to_s3(file_url, filename):
    s3 = boto3.client('s3', aws_access_key_id=os.environ.get("AWS_KEY"),
                      aws_secret_access_key=os.environ.get("AWS_SECRET"))
    bucket_name = 'mjd-insper'
    url = f'https://mjd-insper.s3.sa-east-1.amazonaws.com/{filename}'
    with requests.get(file_url, stream=True) as r:
        r.raise_for_status()
        try:
            s3.upload_fileobj(r.raw, bucket_name, filename,
                              ExtraArgs={'ACL': 'public-read'})
            print(f"Arquivo {filename} enviado.")
        except FileNotFoundError:
            print("Arquivo não encontrado")
    return url


def adicionar_disciplina(nome="Datavis Studio II", zoom_id=91739934274, turma="MJD002", channel="5-tri-interfaces-narrativas-para-web"):
    db = initiate_mongo_db()
    db.disciplinas.insert_one({"finalizada": False, 'nome': nome,
                               "turma": turma, "zoom_id": zoom_id,
                               "channel": "5-tri-interfaces-narrativas-para-web"})


def split_markdown(markdown_str, chunk_size=2000):
    chunks = []
    while len(markdown_str) > chunk_size:
        # Find the last newline before the chunk limit
        split_pos = markdown_str.rfind('\n', 0, chunk_size)
        if split_pos == -1:
            split_pos = chunk_size  # No newline found, split at chunk_size
        chunks.append(markdown_str[:split_pos])
        markdown_str = markdown_str[split_pos:]
    chunks.append(markdown_str)  # Add the last chunk
    return chunks

# UTIL
def markdown_to_slack(markdown_text):
    # Bold text
    slack_text = re.sub(r'\*\*(.*?)\*\*', r'*\1*', markdown_text)
    slack_text = re.sub(r'__(.*?)__', r'*\1*', slack_text)

    # Italic text
    slack_text = re.sub(r'\*(.*?)\*', r'_\1_', slack_text)
    slack_text = re.sub(r'_(.*?)_', r'_\1_', slack_text)

    # Strikethrough text
    slack_text = re.sub(r'~~(.*?)~~', r'~\1~', slack_text)

    # Inline code
    slack_text = re.sub(r'`([^`]+)`', r'`\1`', slack_text)

    # Code block
    slack_text = re.sub(r'```([^`]+)```', r'```\1```', slack_text)

    # Links
    slack_text = re.sub(r'\[(.*?)\]\((.*?)\)', r'<\2|\1>', slack_text)

    # Headings
    slack_text = re.sub(r'^###### (.*?)$', r'*\1*',
                        slack_text, flags=re.MULTILINE)
    slack_text = re.sub(r'^##### (.*?)$', r'*\1*',
                        slack_text, flags=re.MULTILINE)
    slack_text = re.sub(r'^#### (.*?)$', r'*\1*',
                        slack_text, flags=re.MULTILINE)
    slack_text = re.sub(r'^### (.*?)$', r'*\1*',
                        slack_text, flags=re.MULTILINE)
    slack_text = re.sub(r'^## (.*?)$', r'*\1*', slack_text, flags=re.MULTILINE)
    slack_text = re.sub(r'^# (.*?)$', r'*\1*', slack_text, flags=re.MULTILINE)

    # Lists
    slack_text = re.sub(r'^\* (.*?)$', r'• \1', slack_text, flags=re.MULTILINE)
    slack_text = re.sub(r'^\+ (.*?)$', r'• \1', slack_text, flags=re.MULTILINE)
    slack_text = re.sub(r'^\- (.*?)$', r'• \1', slack_text, flags=re.MULTILINE)
    slack_text = re.sub(r'^\d+\. (.*?)$', r'1. \1',
                        slack_text, flags=re.MULTILINE)

    return slack_text


if __name__ == '__main__':
    db = initiate_mongo_db()
    zoom_client = initiate_zoom_app()
    # slack_client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))
    for x in db.disciplinas.find({"finalizada": False}):
        last = get_meeting_info(zoom_client[0], x['zoom_id'], zoom_client[1])
        if db.gravacoes.find_one({"recording_id": last['recording_id']}):
            print("Gravação já consta no banco de dados")
            continue
        else:
            print("Acrescentando gravação")
            if x['turma'] == 'MJD002':
                slack_client = WebClient(
                    token=os.environ.get("SLACK_BOT_TOKEN22"))
                lp = lista_presenca(zoom_client[0], last['meeting_id'])
                file_url = last['download_url']
                filename = f'{x["channel"]}_{last["data_str"].replace("/", "-")}.mp4'
                aws_url = send_large_file_to_s3(file_url, filename)
                last['download_url'] = aws_url
                url_presenca = cria_lista_presenca(
                    last['disciplina'], last['data'], lp['presenca_total'], lp['presenca_parcial'])
                msg_nova_gravacao(last, slack_client,
                                  url_presenca, channel=x['channel'])
                last.update(lp)
                db.gravacoes.insert_one(last)
            elif x['turma'] == 'MJD003':
                print(x)
                slack_client = WebClient(
                    token=os.environ.get("SLACK_BOT_TOKEN23"))
                lp = lista_presenca(zoom_client[0], last['meeting_id'])
                file_url = last['download_url']
                filename = f'{x["channel"]}_{last["data_str"].replace("/", "-")}.mp4'
                aws_url = send_large_file_to_s3(file_url, filename)
                last['download_url'] = aws_url
                url_presenca = cria_lista_presenca(
                    last['disciplina'], last['data'], lp['presenca_total'], lp['presenca_parcial'])
                msg_nova_gravacao(last, slack_client,
                                  url_presenca, channel=x['channel'])
                last.update(lp)
                db.gravacoes.insert_one(last)

                if last['transcription']:
                    print("Generating AI summary")
                    client, token, refresh_token = initiate_zoom_app()
                    new_url = f'{last["transcription"].split("?")[0]}?access_token={token}'
                    transcript = requests.get(new_url)
                    text = transcript.text
                    ai_summary = generate_class_summary(text)
                    parsed = parse_summary(ai_summary)
                    final_summary = fix_class_summary(parsed['blocks'], x['nome'])
                    summary_dict = {
                        "title": final_summary.parsed.title,
                        "summary": final_summary.parsed.summary,
                        "blocks": parsed['blocks']
                    }
                    print("AI summary generated")
                    db.gravacoes.update_one({"recording_id": last["recording_id"]}, {"$set": {"ai_summary": summary_dict}})
                    print("Sending summary to slack")
                    if len(parsed['summary']) >= 2500:
                        chunks = split_markdown(parsed['summary'], chunk_size=2000)
                        for i, chunk in enumerate(chunks):
                            msg_nova_transcricao(markdown_to_slack(chunk), slack_client, channel=x['channel'])
                            time.sleep(2)
                    else:
                        msg_nova_transcricao(parsed['summary'], slack_client, channel=x['channel'])
    print("Done")
            