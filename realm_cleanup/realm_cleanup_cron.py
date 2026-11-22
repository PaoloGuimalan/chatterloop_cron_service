"""Read committed deletion markers; only the existing worker performs cleanup."""
import argparse
import json
import logging
import os
from urllib.parse import quote_plus

QUEUE = 'realm_deletion'
BATCH_SIZE = 100
logger = logging.getLogger(__name__)


def reconcile(cursor, *, apply=False, publish=None):
    after = ''
    count = 0
    while True:
        cursor.execute('''SELECT id, deletion_requested_at FROM community_realm
            WHERE deletion_requested_at IS NOT NULL AND id > %s
            ORDER BY id LIMIT %s''', (after, BATCH_SIZE))
        rows = cursor.fetchall()
        for realm_id, requested_at in rows:
            payload = {'realm_id': str(realm_id), 'deletion_requested_at': requested_at.isoformat()}
            if apply:
                publish(payload)
            else:
                logger.info('Pending deletion: %s', json.dumps(payload))
            count += 1
        if len(rows) < BATCH_SIZE:
            return count
        after = str(rows[-1][0])


def main():
    import pika
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='enqueue pending deletion intents')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    db = psycopg2.connect(host=os.getenv('DB_HOST'), dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'), password=os.getenv('DB_PASS'), port=int(os.getenv('DB_PORT', 5432)),
        sslmode='require', connect_timeout=30, application_name='realm-cleanup-cron')
    broker = None
    try:
        db.set_session(readonly=True, autocommit=True)
        publish = None
        if args.apply:
            params = pika.URLParameters('{}://{}:{}@{}:{}/{}?heartbeat=60'.format(
                os.getenv('RABBITMQ_PROTOCOL', 'amqp'), quote_plus(os.environ['RABBITMQ_USER']),
                quote_plus(os.environ['RABBITMQ_PASS']), os.environ['RABBITMQ_HOST'],
                os.environ['RABBITMQ_PORT'], quote_plus(os.environ['RABBITMQ_VHOST'])))
            broker = pika.BlockingConnection(params)
            channel = broker.channel()
            channel.queue_declare(queue=QUEUE, durable=True)
            channel.confirm_delivery()
            def publish(payload):
                channel.basic_publish(exchange='', routing_key=QUEUE, mandatory=True,
                    body=json.dumps(payload), properties=pika.BasicProperties(delivery_mode=2, content_type='application/json'))
        with db.cursor() as cursor:
            count = reconcile(cursor, apply=args.apply, publish=publish)
        logger.info('%s %s committed deletion intents', 'Enqueued' if args.apply else 'Listed', count)
    finally:
        if broker is not None:
            broker.close()
        db.close()


if __name__ == '__main__':
    main()
