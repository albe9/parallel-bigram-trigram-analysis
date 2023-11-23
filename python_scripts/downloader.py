import requests
import threading
import math
import queue
import os

ABS_PATH = os.path.abspath(__file__)
TEXT_DATA_PATH = f"{os.path.abspath(os.path.join(os.path.dirname(ABS_PATH), os.pardir))}/text_data"

def download_ebook(thread_idx, ebook_range, progress_queue):
    for ebook_idx in ebook_range:
        ebook_data = requests.get(f"https://www.gutenberg.org/ebooks/{ebook_idx + 1}.txt.utf-8")
        ebook_txt = ebook_data.text

        with open(f"{TEXT_DATA_PATH}/ebook{ebook_idx + 1}.txt", "w") as f:
            f.write(ebook_txt)
        progress_queue.put(1)
            

def main():
    EBOOKS_TO_DOWNLOAD=1
    NUM_THREAD = 30
    threads = []
    actual_progress = 0
    progress_queue = queue.Queue(EBOOKS_TO_DOWNLOAD)
    

    if NUM_THREAD > EBOOKS_TO_DOWNLOAD:
        NUM_THREAD = EBOOKS_TO_DOWNLOAD
    

    for i in range(NUM_THREAD):
        if i == NUM_THREAD-1:
            thread = threading.Thread(target=download_ebook, args=(i, range(i * math.floor(EBOOKS_TO_DOWNLOAD/NUM_THREAD),
                                                                       EBOOKS_TO_DOWNLOAD), progress_queue))
        else:
            thread = threading.Thread(target=download_ebook, args=(i, range(i * math.floor(EBOOKS_TO_DOWNLOAD/NUM_THREAD),
                                                                       (i+1) * math.floor(EBOOKS_TO_DOWNLOAD/NUM_THREAD)), progress_queue))
        thread.start()
        threads.append(thread)

    while actual_progress < EBOOKS_TO_DOWNLOAD:
        progress_queue.get()
        actual_progress += 1
        print(f"Progress : [{actual_progress}/{EBOOKS_TO_DOWNLOAD}]")

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()


