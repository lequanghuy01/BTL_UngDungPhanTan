import pika
import json

# Mã băm SHA-256 của chữ "phenikaa2024" (Đây là mục tiêu chúng ta cần giải mã)
TARGET_HASH = "4b8408a2fc212df83ce212cbbeab62a9d80362f6b86ce0dccafb8bda5a782a20"

def main():
    # 1. Kết nối tới RabbitMQ Server
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # 2. Khai báo 2 hàng đợi (Queue): 1 để giao việc, 1 để nhận kết quả
    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_declare(queue='result_queue')

    # 3. Tạo một từ điển giả lập (Trong thực tế bạn sẽ đọc từ file txt hàng triệu dòng)
    dictionary = ["123456", "password", "admin", "phenikaa2023", "phenikaa2024", "qwerty", "iloveyou", "root"]
    
    # Chia nhỏ từ điển thành các gói (Ví dụ mỗi gói 2 từ để test, thực tế là 10.000 từ/gói)
    CHUNK_SIZE = 2
    chunks = [dictionary[i:i + CHUNK_SIZE] for i in range(0, len(dictionary), CHUNK_SIZE)]

    # 4. Phân phối việc lên RabbitMQ
    print("[*] MASTER: Đang chia nhỏ và gửi công việc lên hàng đợi...")
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
                delivery_mode=2,  # Đảm bảo message không bị mất nếu RabbitMQ sập
            ))
    print(f"[*] MASTER: Đã gửi {len(chunks)} gói công việc cho các Slaves.")

    # 5. Hàm xử lý khi nhận được tin vui từ Slave
    def on_result(ch, method, properties, body):
        result = json.loads(body)
        print(f"\n[!!!] MASTER NHẬN KẾT QUẢ: Mật khẩu là '{result['password']}' được tìm thấy bởi {result['found_by']}")
        print("[*] MASTER: Ra lệnh dừng hệ thống (Tắt lắng nghe).")
        channel.stop_consuming() # Ngừng chờ đợi vì đã tìm ra pass

    # 6. Lắng nghe kết quả trả về
    channel.basic_consume(queue='result_queue', on_message_callback=on_result, auto_ack=True)
    print("\n[*] MASTER: Đang chờ các Slaves báo cáo kết quả. Nhấn CTRL+C để thoát.")
    channel.start_consuming()

if __name__ == '__main__':
    main()