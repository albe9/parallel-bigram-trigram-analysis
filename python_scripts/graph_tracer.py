import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import json
import os

ABS_PATH = os.path.abspath(__file__)
MEDIA_MAIN_PATH = f"{os.path.abspath(os.path.join(os.path.dirname(ABS_PATH), os.pardir))}/media"
MEDIA_PATH_CPP = MEDIA_MAIN_PATH + "/cpp_version"
MEDIA_PATH_PYTHON = MEDIA_MAIN_PATH + "/python_version"

def trace_time_ebooks_graph(benchmarks_data, cpp:bool):

    x_axis = [benchmark["ebook_num"] for benchmark in benchmarks_data]

    y_axis_seq = [round(sum(benchmark["seq_timings"])/len(benchmark["seq_timings"]), 3) for benchmark in benchmarks_data]
    y_axis_par = [round(sum(benchmark["par_timings"])/len(benchmark["par_timings"]), 3) for benchmark in benchmarks_data]
    if cpp:
        y_axis_parV2 = [round(sum(benchmark["par_timings_V2"])/len(benchmark["par_timings_V2"]), 3) for benchmark in benchmarks_data]
    
    if cpp:
        num_thread = benchmarks_data[0]["threads_num"]
    else:
        num_process = benchmarks_data[0]["process_num"]

    fig, ax = plt.subplots(figsize=(8, 8))

    ax.plot(x_axis, y_axis_seq, label="Sequential",)
    ax.plot(x_axis, y_axis_par, label="Parallel")
    if cpp:
        ax.plot(x_axis, y_axis_parV2, label="Parallel V2")
    ax.set_xscale('log')
    ax.set_yscale('log')

    # plot vertical lines
    differences = [(y_seq / y_par) for y_seq, y_par in zip(y_axis_seq, y_axis_par)]

    for x, y_seq, y_par, diff in zip(x_axis, y_axis_seq, y_axis_par, differences):
        ax.plot([x, x], [y_seq, y_par], 'r--')
        ax.text(x + 0.1, (y_seq + y_par) / 2, f"{round(diff, 2)}", color='red')

    custom_legend = Line2D([0], [0], color='red', linestyle='--', linewidth=2, markersize=5)

    ax.set_xlabel('Number of ebook analysed')
    ax.set_ylabel('Time [s]')
    if cpp:
        ax.set_title(f'Benchmarks [{num_thread} threads]')
    else:
        ax.set_title(f'Benchmarks [{num_process} process]')

    # Combine default and custom legends
    handles, labels = plt.gca().get_legend_handles_labels()
    handles.append(custom_legend)
    labels.append('Speed_up')

    ax.legend(handles=handles, labels=labels, loc='upper left')
    if cpp:
        fig.savefig(f"{MEDIA_PATH_CPP}/benchmarks_ebooks.png", dpi=600)
    else:
        fig.savefig(f"{MEDIA_PATH_PYTHON}/benchmarks_ebooks.png", dpi=600)

def trace_time_threads_graph(benchmarks_data, cpp:bool):
    if cpp:
        x_axis = [benchmark["threads_num"] for benchmark in benchmarks_data]
    else:
        x_axis = [benchmark["process_num"] for benchmark in benchmarks_data]
        
    y_axis_seq  = [round(sum(benchmarks_data[0]["seq_timings"])/len(benchmarks_data[0]["seq_timings"]), 3) for _ in benchmarks_data]
    y_axis_par = [round(sum(benchmark["par_timings"])/len(benchmark["par_timings"]), 3) for benchmark in benchmarks_data]
    if cpp:
        y_axis_parV2 = [round(sum(benchmark["par_timings_V2"])/len(benchmark["par_timings_V2"]), 3) for benchmark in benchmarks_data]
    
    num_ebooks = benchmarks_data[0]["ebook_num"]

    fig, ax = plt.subplots(figsize=(8, 8))

    ax.plot(x_axis, y_axis_seq, label="Sequential",)
    ax.plot(x_axis, y_axis_par, label="Parallel")
    if cpp:
        ax.plot(x_axis, y_axis_parV2, label="Parallel V2")

    # plot vertical lines
    differences = [(y_seq / y_par) for y_seq, y_par in zip(y_axis_seq, y_axis_par)]

    for x, y_seq, y_par, diff in zip(x_axis, y_axis_seq, y_axis_par, differences):
        ax.plot([x, x], [y_seq, y_par], 'r--')
        ax.text(x + 0.1, (y_seq + y_par) / 2, f"{round(diff, 2)}", color='red')

    custom_legend = Line2D([0], [0], color='red', linestyle='--', linewidth=2, markersize=5)

    if cpp:
        ax.set_xlabel('Number of threads utilized')
    else:
        ax.set_xlabel('Number of process utilized')
    ax.set_ylabel('Time [s]')
    ax.set_title(f'Benchmarks [{num_ebooks} ebooks]')

    # Combine default and custom legends
    handles, labels = plt.gca().get_legend_handles_labels()
    handles.append(custom_legend)
    labels.append('Speed_up')

    ax.legend(handles=handles, labels=labels, loc='lower left')
    if cpp:
        fig.savefig(f"{MEDIA_PATH_CPP}/benchmarks_threads.png", dpi=600)
    else:
        fig.savefig(f"{MEDIA_PATH_PYTHON}/benchmarks_process.png", dpi=600)


def main():

    # # Cpp graphs
    # # load benchmarks_ebooks data
    # with open(f"{MEDIA_PATH_CPP}/benchmarks_ebooks.json",'r') as json_file:
    #     benchmarks_ebooks_data = json.load(json_file)
    # #trace graph
    # trace_time_ebooks_graph(benchmarks_ebooks_data, True)

    # load benchmarks_threads data
    with open(f"{MEDIA_PATH_CPP}/benchmarks_threads.json",'r') as json_file:
        benchmarks_threads_data = json.load(json_file)
    #trace graph
    trace_time_threads_graph(benchmarks_threads_data, True)


    # # Python graphs
    # # load benchmarks_ebooks data
    # with open(f"{MEDIA_PATH_PYTHON}/benchmarks_ebooks.json",'r') as json_file:
    #     benchmarks_ebooks_data = json.load(json_file)
    # #trace graph
    # trace_time_ebooks_graph(benchmarks_ebooks_data, False)

    # # load benchmarks_process data
    # with open(f"{MEDIA_PATH_PYTHON}/benchmarks_process.json",'r') as json_file:
    #     benchmarks_process_data = json.load(json_file)
    # #trace graph
    # trace_time_threads_graph(benchmarks_process_data, False)

if __name__ == "__main__":
    main()