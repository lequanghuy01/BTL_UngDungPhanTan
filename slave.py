import pika
import json
import hashlib
import time
import os

# Lấy ID của Process để phân biệt các Slave với nhau lúc Demo
SLAVE_ID = f"Slave-PID-{os.getpid()}"

def main():
    # 1. Kết nối tới RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Khai báo lại Queue y hệt Master (đề phòng trường hợp Slave chạy trước Master)
    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_declare(queue='result_queue')

    print(f"[*] {SLAVE_ID}: Đã khởi động và đang chờ việc...")

    # 2. Hàm thực thi khi Slave nhận được 1 gói công việc
    def callback(ch, method, properties, body):
        task = json.loads(body)
        target_hash = task['target_hash']
        words = task['words']

        print(f"[-] {SLAVE_ID}: Đang dò quét gói chứa {len(words)} mật khẩu... {words}")
        
        found = False
        for word in words:
            # Băm mật khẩu bằng thuật toán SHA-256
            hashed = hashlib.sha256(word.encode('utf-8')).hexdigest()
            
            # So sánh mã băm
            if hashed == target_hash:
                print(f"[!] {SLAVE_ID}: BINGO! Đã tìm thấy mật khẩu: {word}")
                
                # Gửi thông báo về cho Master qua hàng đợi 'result_queue'
                result_msg = {'password': word, 'found_by': SLAVE_ID}
                channel.basic_publish(exchange='', routing_key='result_queue', body=json.dumps(result_msg))
                found = True
                break
        
        # Giả lập độ trễ tính toán để dễ quan sát (0.35 giây)
        time.sleep(0.35)
        
        # Báo cáo với RabbitMQ là "Tôi đã làm xong gói này, hãy xóa nó khỏi hàng đợi"
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # 3. Kỹ thuật "Fair Dispatch" (Điều phối công bằng)
    # Chỉ giao 1 gói việc mới cho Slave nếu Slave này đã làm xong gói cũ
    channel.basic_qos(prefetch_count=1)
    
    # 4. Lắng nghe việc từ Master
    channel.basic_consume(queue='task_queue', on_message_callback=callback)
    channel.start_consuming()

if __name__ == '__main__':
    main()