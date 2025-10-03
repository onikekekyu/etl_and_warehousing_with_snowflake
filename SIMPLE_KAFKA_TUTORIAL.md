# 🚀 Simple Kafka Tutorial (No Docker Required)

This tutorial teaches Kafka concepts using a **Python-based implementation** that doesn't require Docker. Perfect for learning Kafka fundamentals without infrastructure complexity.

## 🎯 What You have Learn (I hope)

- **Kafka fundamentals**: Topics, producers, consumers, (definition in the slides)
- **Real-time streaming**: Live data processing concepts
- **Message reliability**: Delivery guarantees and error handling
- **Scalability concepts**: Multiple consumers and load balancing

## 📋 Prerequisites

- Python 3.8+ installed
- Basic understanding of JSON and Python
- No Docker required!

## 🚀 Step 1: Test the Simple Kafka Setup

### 1.1 Basic Functionality Test

create this file

Simple_kafka_setup.py


import json
import time
import threading
import queue
import sys
import os
from datetime import datetime
from typing import Dict, List, Any

class SimpleKafkaBroker:
    """Simple Kafka-like message broker using Python queues"""
    
    def __init__(self):
        self.topics: Dict[str, queue.Queue] = {}
        self.consumers: Dict[str, List[queue.Queue]] = {}
        self.running = True
        
    def create_topic(self, topic_name: str, partitions: int = 1):
        """Create a new topic"""
        if topic_name not in self.topics:
            self.topics[topic_name] = queue.Queue()
            self.consumers[topic_name] = []
            print(f"✅ Created topic: {topic_name}")
        else:
            print(f"ℹ️  Topic {topic_name} already exists")
    
    def publish_message(self, topic_name: str, message: str) -> bool:
        """Publish a message to a topic"""
        if topic_name not in self.topics:
            print(f"❌ Topic {topic_name} does not exist")
            return False
        
        try:
            # Add timestamp and message ID
            message_data = {
                'id': f"{int(time.time() * 1000)}",
                'timestamp': datetime.now().isoformat(),
                'data': message,
                'topic': topic_name
            }
            
            # Put message in topic queue
            self.topics[topic_name].put(message_data)
            print(f"📤 Published to {topic_name}: {message[:50]}...")
            return True
            
        except Exception as e:
            print(f"❌ Error publishing message: {e}")
            return False
    
    def subscribe_to_topic(self, topic_name: str, consumer_id: str) -> queue.Queue:
        """Subscribe to a topic and return a consumer queue"""
        if topic_name not in self.topics:
            self.create_topic(topic_name)
        
        # Create consumer queue
        consumer_queue = queue.Queue()
        self.consumers[topic_name].append(consumer_queue)
        
        # Start consumer thread
        consumer_thread = threading.Thread(
            target=self._consumer_worker,
            args=(topic_name, consumer_queue, consumer_id),
            daemon=True
        )
        consumer_thread.start()
        
        print(f"👥 Consumer {consumer_id} subscribed to {topic_name}")
        return consumer_queue
    
    def _consumer_worker(self, topic_name: str, consumer_queue: queue.Queue, consumer_id: str):
        """Worker thread for consuming messages"""
        while self.running:
            try:
                # Get message from topic
                message = self.topics[topic_name].get(timeout=1.0)
                
                # Forward to consumer queue
                consumer_queue.put(message)
                
                print(f"📥 Consumer {consumer_id} received: {message['data'][:50]}...")
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"❌ Consumer {consumer_id} error: {e}")
                break
    
    def get_topic_stats(self, topic_name: str) -> Dict[str, Any]:
        """Get statistics for a topic"""
        if topic_name not in self.topics:
            return {"error": "Topic not found"}
        
        return {
            "topic": topic_name,
            "queue_size": self.topics[topic_name].qsize(),
            "consumer_count": len(self.consumers[topic_name]),
            "status": "active"
        }
    
    def list_topics(self) -> List[str]:
        """List all topics"""
        return list(self.topics.keys())
    
    def stop(self):
        """Stop the broker"""
        self.running = False
        print("🛑 Kafka broker stopped")

# Global broker instance
broker = SimpleKafkaBroker()

class SimpleKafkaProducer:
    """Simple Kafka producer"""
    
    def __init__(self, broker_instance):
        self.broker = broker_instance
        self.message_count = 0
    
    def publish(self, topic_name: str, message: str) -> bool:
        """Publish a message"""
        success = self.broker.publish_message(topic_name, message)
        if success:
            self.message_count += 1
        return success
    
    def flush(self):
        """Flush any pending messages (no-op in simple implementation)"""
        pass

class SimpleKafkaConsumer:
    """Simple Kafka consumer"""
    
    def __init__(self, broker_instance, consumer_id: str = None):
        self.broker = broker_instance
        self.consumer_id = consumer_id or f"consumer_{int(time.time())}"
        self.consumer_queue = None
        self.message_count = 0
    
    def subscribe(self, topic_name: str):
        """Subscribe to a topic"""
        self.consumer_queue = self.broker.subscribe_to_topic(topic_name, self.consumer_id)
        print(f"📡 Subscribed to {topic_name}")
    
    def poll(self, timeout: float = 1.0) -> Dict[str, Any]:
        """Poll for messages"""
        if not self.consumer_queue:
            return None
        
        try:
            message = self.consumer_queue.get(timeout=timeout)
            self.message_count += 1
            return message
        except queue.Empty:
            return None
        except Exception as e:
            print(f"❌ Consumer poll error: {e}")
            return None
    
    def close(self):
        """Close the consumer"""
        print(f"🔒 Consumer {self.consumer_id} closed")

def main():
    """Main function for testing the simple Kafka setup"""
    print("🚀 Simple Kafka Setup (No Docker Required)")
    print("=" * 60)
    
    # Create topic
    topic_name = "kpop_merchandise_orders"
    broker.create_topic(topic_name)
    
    # Create producer
    producer = SimpleKafkaProducer(broker)
    
    # Create consumer
    consumer = SimpleKafkaConsumer(broker)
    consumer.subscribe(topic_name)
    
    print(f"\n📊 Testing message flow...")
    
    # Test publishing messages
    test_messages = [
        '{"txid": "test1", "item": "Kpop Demon Hunter T-Shirt", "name": "Alice"}',
        '{"txid": "test2", "item": "Kpop Demon Hunter Hoodie", "name": "Bob"}',
        '{"txid": "test3", "item": "Kpop Demon Hunter Poster", "name": "Charlie"}'
    ]
    
    for message in test_messages:
        producer.publish(topic_name, message)
        time.sleep(0.5)  # Small delay between messages
    
    # Test consuming messages
    print(f"\n📥 Consuming messages...")
    for i in range(5):  # Try to consume 5 messages
        message = consumer.poll(timeout=2.0)
        if message:
            print(f"   Message {i+1}: {message['data']}")
        else:
            print(f"   No message received (timeout)")
    
    # Show statistics
    stats = broker.get_topic_stats(topic_name)
    print(f"\n📊 Topic Statistics:")
    print(f"   Topic: {stats['topic']}")
    print(f"   Queue size: {stats['queue_size']}")
    print(f"   Consumer count: {stats['consumer_count']}")
    print(f"   Messages published: {producer.message_count}")
    print(f"   Messages consumed: {consumer.message_count}")
    
    # Cleanup
    consumer.close()
    broker.stop()
    
    print(f"\n✅ Simple Kafka test completed!")

if __name__ == "__main__":
    main()


THEN TEST IT

```bash
# Test the simple Kafka implementation
python simple_kafka_setup.py
```

## Expected Output 
```
🚀 Simple Kafka Setup (No Docker Required)
✅ Created topic: kpop_merchandise_orders
📤 Published to kpop_merchandise_orders: {"txid": "test1", "item": "Kpop Demon Hunter T-Shirt"...
📤 Published to kpop_merchandise_orders: {"txid": "test2", "item": "Kpop Demon Hunter Hoodie"...
📤 Published to kpop_merchandise_orders: {"txid": "test3", "item": "Kpop Demon Hunter Poster"...
👥 Consumer consumer_1234567890 subscribed to kpop_merchandise_orders
📡 Subscribed to kpop_merchandise_orders
📥 Consumer consumer_1234567890 received: {"txid": "test1", "item": "Kpop Demon Hunter T-Shirt"...
📥 Consumer consumer_1234567890 received: {"txid": "test2", "item": "Kpop Demon Hunter Hoodie"...
📥 Consumer consumer_1234567890 received: {"txid": "test3", "item": "Kpop Demon Hunter Poster"...
```

## 🐍 Step 2: Test Publishing Messages

### 2.1 Publish Sample Data

```bash
# Set the topic name (simple one please)
export KAFKA_TOPIC=kpop_merchandise_orders

# Publish 1 test message
python data_generator.py 1 | python simple_publish_data.py
```

**Expected Output:**
```
🚀 Simple Kafka Publisher for Kpop Demon Hunter Merchandise
📊 Topic: kpop_merchandise_orders
📡 Publishing messages... (Press Ctrl+C to stop)
📤 Published to kpop_merchandise_orders: {"txid": "123e4567-e89b-12d3-a456-426614174000"...
📊 Publishing Complete!
   ✅ Messages published: 1
   ❌ Messages failed: 0
   📈 Success rate: 100.0%

📊 Topic Statistics:
   Queue size: 1
   Consumer count: 0
```

### 2.2 Publish Multiple Messages

```bash
# Publish 10 messages
python data_generator.py 10 | python simple_publish_data.py
```

## 📡 Step 3: Test Consuming Messages

### 3.1 Start a Consumer

```bash
# In a new terminal, start consuming messages
python simple_consume_data.py
```

**Expected Output:**
```
🚀 Simple Kafka Consumer for Kpop Demon Hunter Merchandise
📊 Topic: kpop_merchandise_orders
📡 Waiting for messages... (Press Ctrl+C to stop)
```

### 3.2 Publish Messages (in another terminal)

```bash
# Publish 5 messages
python data_generator.py 5 | python simple_publish_data.py
```

**You should see the consumer display:**
```
📦 Order #1
   🆔 Transaction ID: 123e4567-e89b-12d3-a456-426614174000
   🛍️  Product: Kpop Demon Hunter T-Shirt
   👤 Customer: John Smith
   📧 Email: john.smith@email.com
   🕐 Purchase Time: 2024-01-15T10:30:00.000000
   🏠 Address: 123 Main St, New York, NY 10001
   📍 Message ID: 1705312200000
```

## 🎮 Step 4: Advanced Testing (optional, will be useful for session 4)

### 4.1 High-Volume Publishing

```bash
# Publish 100 messages
python data_generator.py 100 | python simple_publish_data.py
```

### 4.2 Multiple Consumers

```bash
# Terminal 1: Start consumer 1
python simple_consume_data.py

# Terminal 2: Start consumer 2 (in another terminal)
export KAFKA_CONSUMER_GROUP=consumer_2
python simple_consume_data.py

# Terminal 3: Publish messages
python data_generator.py 50 | python simple_publish_data.py
```

**What happens:**
- Messages are distributed between consumers (remember exemple like La poste, the queue in class, football game....)
- Each consumer processes different messages
- The system handles load balancing automatically

## 🔧 Step 5: Understanding Kafka Concepts

### 5.1 Key Concepts ( tableau recap )

| Concept | Description | Simple Implementation |
|---------|-------------|----------------------|
| **Topic** | Category for messages | Python queue |
| **Producer** | Sends messages | `simple_publish_data.py` |
| **Consumer** | Receives messages | `simple_consume_data.py` |
| **Message** | Data being sent | JSON string |
| **Queue** | Message storage | Python `queue.Queue` |

### 5.2 Message Flow

```
Data Generator → Producer → Topic Queue → Consumer → Display
     ↓              ↓           ↓           ↓
  JSON Data    Message      Queue      Real-time
  Records      Publishing    Storage    Processing
```


## 📊 Step 6: Monitor Performance

### 6.1 Check Topic Statistics

```python
# Run this in Python to check topic stats
from simple_kafka_setup import broker

# Get topic statistics
stats = broker.get_topic_stats("kpop_merchandise_orders")
print(f"Topic: {stats['topic']}")
print(f"Queue size: {stats['queue_size']}")
print(f"Consumer count: {stats['consumer_count']}")
```

### 6.2 Performance Testing

```bash
# Test with different volumes
python data_generator.py 10 | python simple_publish_data.py
python data_generator.py 100 | python simple_publish_data.py
python data_generator.py 1000 | python simple_publish_data.py
```



## 🚨 help with troubleshooting

### 8.1 Common Issues

**"Topic not found"**
```bash
# Check if topic exists
python -c "from simple_kafka_setup import broker; print(broker.list_topics())"
```

**"No messages received"**
- Check if messages were published successfully
- Verify consumer is subscribed to correct topic
- Check for JSON parsing errors

**"Consumer not responding"**
- Press Ctrl+C to stop and restart consumer
- Check for error messages in output

### 8.2 Debug Commands

```python
# Check broker status
from simple_kafka_setup import broker

# List all topics
print("Topics:", broker.list_topics())

# Get topic statistics
stats = broker.get_topic_stats("kpop_merchandise_orders")
print("Stats:", stats)
```

## 🎯 Performance Optimization (optional)

### 9.1 Producer Optimization

```python
# Optimize for throughput
config = {
    'batch_size': 100,        # Process in batches
    'timeout': 1.0,           # Shorter timeouts
    'retry_count': 3,          # Retry failed messages
}
```

### 9.2 Consumer Optimization

```python
# Optimize for throughput
config = {
    'poll_timeout': 0.1,      # Shorter poll timeouts
    'batch_processing': True,  # Process multiple messages
    'error_handling': True,    # Handle errors gracefully
}
```


## 🎉 Key Takeaways WELLL DONE CLASS

1. **Kafka enables real-time streaming** - not batch processing
2. **Topics organize messages** - like categories or channels
3. **Producers send messages** - consumers receive them
4. **Message reliability** - messages are queued and delivered
5. **Scalability** - multiple consumers can process messages

## 🚀 Next Steps

- **Install Docker** when ready for real Kafka
- **Scale up**: Use real Kafka for production
- **Integrate**: Connect to Snowflake via Kafka Connect
- **Monitor**: Set up production monitoring
- **Deploy**: Move to cloud Kafka services

## 📚 Additional Resources

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Docker Installation Guide](INSTALL_DOCKER.md)
- [Real Kafka Tutorial](KAFKA_TUTORIAL.md)

---

**🎉 Congratulations!** You've learned Kafka concepts without Docker complexity. This foundation will help you understand real Kafka when you're ready to scale up! 🚀
