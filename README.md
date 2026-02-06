build:

- docker build -t johnpauloramil187/chatterloop_cron_service .

test:

- docker run --rm johnpauloramil187/chatterloop_cron_service python post_scores_cron.py --test
