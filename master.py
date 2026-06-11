import pika
import json
import os

# Mã băm SHA-256 của chữ "phenikaa2024"
TARGET_HASH = "4b8408a2fc212df83ce212cbbeab62a9d80362f6b86ce0dccafb8bda5a782a20"

def main():
    # 1. Kết nối tới RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_declare(queue='result_queue')

    # 2. ĐỌC TỪ FILE TXT THAY VÌ MẢNG CỐ ĐỊNH
    file_path = 'passwords.txt'
    if not os.path.exists(file_path):
        print(f"[!] Lỗi: Không tìm thấy file {file_path}. Hãy tạo file này nhé!")
        return

    # Mở file và đọc tất cả các dòng, loại bỏ khoảng trắng/xuống dòng
    with open(file_path, 'r', encoding='utf-8') as f:
        dictionary = [line.strip() for line in f.readlines() if line.strip()]

    print(f"[*] MASTER: Đã tải thành công {len(dictionary)} mật khẩu từ thư viện.")

    # 3. Chia nhỏ khối lượng công việc (Mỗi gói 500 từ)
    CHUNK_SIZE = 10
    chunks = [dictionary[i:i + CHUNK_SIZE] for i in range(0, len(dictionary), CHUNK_SIZE)]

    print(f"[*] MASTER: Đang chia thành {len(chunks)} gói công việc và ném lên RabbitMQ...")
    
    for chunk in chunks:
        task = {
            'target_hash': TARGET_HASH,
            'words': chunk
        }
        channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=json.dumps(task),
            properties=pika.BasicProperties(
                delivery_mode=2, # Chống mất dữ liệu
            ))
    print(f"[*] MASTER: Đã giao việc xong!")

    # 4. Lắng nghe tin vui từ Slave
    def on_result(ch, method, properties, body):
        result = json.loads(body)
        print(f"\n[!!!] MASTER NHẬN KẾT QUẢ: Mật khẩu là '{result['password']}' được tìm ra bởi {result['found_by']}")
        print("[*] MASTER: Nhiệm vụ hoàn thành, ra lệnh dừng toàn bộ hệ thống!")
        channel.stop_consuming()

    channel.basic_consume(queue='result_queue', on_message_callback=on_result, auto_ack=True)
    print("\n[*] MASTER: Đang chờ các Slaves dò tìm... Nhấn CTRL+C để thoát.")
    channel.start_consuming()

if __name__ == '__main__':
    main()