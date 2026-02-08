"""
Cloud Function to process Pub/Sub messages.

    1. Receives a Pub/Sub event triggered by a message published to a Pub/Sub topic.
    2. Read the Pub/Sub messages
        - user_id
        - type
        - episode_id
    3. Processes only CONTINUE_LISTENING messages.
    4. Reads the user’s preferred language from Firestore.
    5. Read the Notification collection in Firestore to select the name.
    6. Selects the correct language template. Note that it is in lowercase.
    7. Replaces template placeholders ({{user_id}}, {{episode_id}}) with real values.
    8. Displays the message in the user's language.

EDEM. Master Big Data & Cloud 2025/2026
Professor: Javi Briones & Adriana Campos
"""


import base64
import json
from google.cloud import firestore

# Initialize Firestore client
firestore_client = firestore.Client()


# C. Python Libraries
import argparse
import json


def notification(event, context):


    """ Input Arguments """

    parser = argparse.ArgumentParser(description=('Input arguments for the Dataflow Streaming Pipeline.'))

    parser.add_argument(
                '--fs_user_collection',
                required=False,
                default="usuarios",
                help='Firestore Collection.')
    
    parser.add_argument(
                '--fs_noti_collection',
                required=False,
                default="notification",
                help='Firestore Collection.')
    
    
    args = parser.parse_known_args()[0]
    """
    Gen2 Cloud Function 
    1. Recibe un evento de Pub/Sub activado por un mensaje publicado en un tópico de Pub/Sub.
    2. Lee los mensajes de Pub/Sub, extrayendo los campos: user_id, type y episode_id.
    """
    if 'data' in event:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        try:
            message_json = json.loads(pubsub_message)
        except Exception as e:
            print(f"Error al decodificar el mensaje: {e}")
            return
        user_id = message_json.get('user_id')
        msg_type = message_json.get('type')
        payload_data = message_json.get('payload', {})
        episode_id = payload_data.get('episode_id', 'Unknown') # Sacamos el ID de ahí
        print(f"Mensaje recibido: user_id={user_id}, type={msg_type}, episode_id={episode_id}")
    else:
        print("No se encontró el campo 'data' en el evento Pub/Sub.")
        return
    
    if msg_type == "CONTINUE_LISTENING":
        doc_ref = firestore_client.collection(args.fs_user_collection).document(user_id)
        doc = doc_ref.get()
        if doc.exists:
            valor = doc.to_dict().get('language')
        doc_ref = firestore_client.collection(args.fs_noti_collection).document('CONTINUE_LISTENING')
        doc = doc_ref.get()
        msg_idioma = f"msg_{valor.lower()}"
        if doc.exists:
            noti = doc.to_dict().get(msg_idioma)
            if noti:
                noti = noti.replace("{{user_id}}", str(user_id)).replace("{{episode_id}}", str(episode_id))
                print(f"Mensaje personalizado: {noti}")

        