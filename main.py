import datetime, requests
from dotenv import load_dotenv
import os

load_dotenv()

def parse_td():
    from xmlrpc.client import DateTime
    from telethon.sync import TelegramClient

    from telethon.tl.functions.messages import GetDialogsRequest
    from telethon.tl.types import InputPeerEmpty
    from telethon.tl.functions.messages import GetHistoryRequest
    from telethon.tl.types import PeerChannel, PeerUser


    import csv

    API_ID = int(os.getenv('API_ID'))
    API_HASH = os.getenv('API_HASH')
    LOG_TO_TELEGRAM_BOT_TOKEN = os.getenv('LOG_TO_TELEGRAM_BOT_TOKEN')
    TICKET_TELEGRAM_CHAT_ID = os.getenv('TICKET_TELEGRAM_CHAT_ID')
    PHONE_NUMBER = os.getenv('PHONE_NUMBER')

    client = TelegramClient(PHONE_NUMBER, API_ID, API_HASH)

    client.start()

    chats = []
    last_date = None
    chunk_size = 200
    groups = []
    result = client(GetDialogsRequest(
        offset_date=last_date,
        offset_id=0,
        offset_peer=InputPeerEmpty(),
        limit=chunk_size,
        hash=0
    ))
    chats.extend(result.chats)
    for chat in chats:
        try:
            if chat.megagroup == True:
                groups.append(chat)
        except:
            continue
    print("Выберите группу для парсинга сообщений:")
    i = 0
    # for g in groups:
        # print(str(i) + "- " + g.title)
        # i += 1
    # g_index = input("Введите нужную цифру: ")
    all_messages = []
    all_message_ids = []
    for target_group in groups:
        i += 1
        print(str(i) + "- " + target_group.title+": ")
        
        offset_id = 0
        limit = 100
        
        print("Начинаем парсинг сообщений...")
        message_count = 0

        while True:
            message_count += 1
            print("message_count: {}".format(message_count))
            
            history = client(GetHistoryRequest(
                peer=target_group,
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=0,
                hash=0
            ))
            if not history.messages:
                break
            messages = history.messages
            for message in messages:
                # print("Сообщение: {}".format(message.message))
                if message.message:
                    msg_text = message.message.lower()
                    if ("#вакансия" in msg_text or "вакансия:" in msg_text) and "добавьте хештег #вакансия" not in msg_text:
                        from_user = message.from_id

                        # Проверка, что объект является экземпляром PeerUser
                        user_data = None
                        if isinstance(from_user, PeerUser):
                            # Получение информации об авторе сообщения
                            user_entity = client.get_entity(from_user)
                            print("user_entity.id: {}".format(user_entity.id))
                            user_data = {                    
                                "user_id":user_entity.id,
                                "first_name":user_entity.first_name,
                                "last_name":user_entity.last_name,
                                "username":user_entity.username,
                            }

                        
                        formatted_datetime = message.date.strftime("%d.%m.%Y %H:%M:%S")
                        message_data = {
                            "tg_group": target_group.title,
                            "message_id": message.id,
                            "message_text": message.message,
                            "user_data": user_data,
                            "message_date": formatted_datetime
                        }
                        if message.id not in all_message_ids:
                            all_message_ids.append(message.id)
                            all_messages.append(message_data)
            offset_id = messages[len(messages) - 1].id        
            if message_count>=10:
                break
        print("i: {}".format(i))
        if i == 5:
            break
          

    print("Сохраняем данные в файл...")

    with open("chats.csv", "w", encoding="UTF-8") as f:
        writer = csv.writer(f, delimiter=",", lineterminator="\n")
        for message in all_messages:
            writer.writerow([message])
            message_text = message.get('message_text')
            cleaned_text = message_text.replace("#", "")

            resp = requests.post(
                f"https://api.telegram.org/bot{LOG_TO_TELEGRAM_BOT_TOKEN}/sendMessage?chat_id={TICKET_TELEGRAM_CHAT_ID}&text={cleaned_text}&parse_mode=HTML"
            )
            
    print('Парсинг сообщений группы успешно выполнен.')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    parse_td()
