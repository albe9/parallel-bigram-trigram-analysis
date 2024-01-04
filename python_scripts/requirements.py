import os


ABS_PATH = os.path.abspath(__file__)
MAIN_GIT_PATH = os.path.abspath(os.path.join(os.path.dirname(ABS_PATH), os.pardir))

def main():
    # Create Output folder and subfolders needed by python and cpp analysis
    path_to_check_list = [
        f"{MAIN_GIT_PATH}/output",
        f"{MAIN_GIT_PATH}/output/cpp_version",
        f"{MAIN_GIT_PATH}/output/python_version",
        f"{MAIN_GIT_PATH}/output/python_version/asyncio",
        f"{MAIN_GIT_PATH}/output/python_version/multi_processing",
        f"{MAIN_GIT_PATH}/output/python_version/multi_threading",
        f"{MAIN_GIT_PATH}/output/python_version/seq"
    ]

    for path_to_check in path_to_check_list:
        if not os.path.exists(path_to_check):
            os.mkdir(path_to_check)
    

if __name__ == "__main__":
    main()
