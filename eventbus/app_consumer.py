from typing import List

from eventbus import config, config_watcher


def consumer_main(consumer_id: str):
    pass


def main():
    from multiprocessing import Process

    consumer_processes: List[Process] = []
    for consumer_id, _ in config.get().consumers.items():
        p = Process(
            target=consumer_main,
            name=f"Consumer#{consumer_id}",
            args=(consumer_id,),
            daemon=True,
        )
        p.start()
        consumer_processes.append(p)

    # watch after processes started
    config_watcher.load_and_watch_file_from_environ()

    for p in consumer_processes:
        p.is_alive()


if __name__ == "__main__":
    main()
