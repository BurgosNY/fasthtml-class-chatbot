import os
import base64
# from llm_summarizer import prepara_resumo, markdown_to_slack
import requests
from unidecode import unidecode
from pyzoom import ZoomClient, refresh_tokens
from slack_sdk import WebClient
from operator import itemgetter
from pymongo import MongoClient
import time
import arrow
import boto3
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


def initiate_zoom_app():
    """
    Initiates the Zoom application by retrieving the Zoom client ID and secret
    from environment variables, refreshing tokens, and returning a ZoomClient instance.

    Raises:
        Exception: If the Zoom client ID or secret is not found in environment variables.

    Returns:
        tuple: A tuple containing the ZoomClient instance and the new access token.
    """
    db = initiate_mongo_db()
    stored_refresh_token = db.utils.find_one(
        {"function": "zoom_refresher"})['token']
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
        token_r = requests.post(
            token_url, data=token_data, headers=token_headers)
        token_r_json = token_r.json()

        # Extract the tokens from the response
        new_token = token_r_json['access_token']
        new_refresh_token = token_r_json['refresh_token']
        db.utils.update_one({"function": "zoom_refresher"}, {
                            "$set": {"token": new_refresh_token}})

        client = ZoomClient(access_token=new_token)  # type: ignore
        return client, new_token, new_refresh_token

    except Exception as e:
        print(f"An error occurred while refreshing Zoom tokens: {e}")
        import traceback
        traceback.print_exc()


def initiate_mongo_db():
    user = os.environ.get('MONGODB_USER')
    psw = os.environ.get('MONGODB_PSW')
    mongo_uri = os.environ.get('MONGODB_URI')
    uri = f'mongodb://{user}:{psw}@{mongo_uri}/mjd?ssl=true'
    db = MongoClient(uri, ssl=True, tlsAllowInvalidCertificates=True).mjd
    return db


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
        obj['transcription'] = f"{transcricao_list[0]
                                  ['download_url']}?access_token={token}"
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
                filename = f'{x["channel"]}_{
                    last["data_str"].replace("/", "-")}.mp4'
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
                filename = f'{x["channel"]}_{
                    last["data_str"].replace("/", "-")}.mp4'
                aws_url = send_large_file_to_s3(file_url, filename)
                last['download_url'] = aws_url
                url_presenca = cria_lista_presenca(
                    last['disciplina'], last['data'], lp['presenca_total'], lp['presenca_parcial'])
                msg_nova_gravacao(last, slack_client,
                                  url_presenca, channel=x['channel'])
                last.update(lp)

                # if last['transcription']:
                #    print("Transcrição disponível")
                #    transcricao = requests.get(last['transcription']).text
                #    descricao_disciplina = x['descricao']
                #    markdown = prepara_resumo(transcricao, descricao_disciplina, last['disciplina'])
                #    if len(markdown) >= 2500:
                #        chunks = split_markdown(markdown, chunk_size=2000)
                #        for i, chunk in enumerate(chunks):
                #            msg_nova_transcricao(markdown_to_slack(chunk), slack_client, channel=x['channel'])
                #            time.sleep(2)
                # else:
                #    print("Transcrição indisponível")
                #    markdown = None
                #    msg_nova_transcricao(markdown, slack_client, channel=x['channel'])
                # last.update({"markdown": markdown})
                db.gravacoes.insert_one(last)
