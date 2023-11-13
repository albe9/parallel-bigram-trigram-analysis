import requests

EBOOKS_TO_DOWNLOAD=100

def main():

    for ebook_idx in range(1, EBOOKS_TO_DOWNLOAD):
        ebook_data = requests.get(f"https://www.gutenberg.org/ebooks/{ebook_idx}.txt.utf-8")
        ebook_txt = ebook_data.text

        with open(f"./text_data/ebook{ebook_idx}.txt", "w") as f:
            f.write(ebook_txt)
            print(f"Progress : [{ebook_idx}/{EBOOKS_TO_DOWNLOAD}]")

if __name__ == "__main__":
    main()


