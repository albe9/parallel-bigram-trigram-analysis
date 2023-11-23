import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import json
import os

ABS_PATH = os.path.abspath(__file__)
MEDIA_PATH = f"{os.path.abspath(os.path.join(os.path.dirname(ABS_PATH), os.pardir))}/media"

def main():

    # load benchmarks data
    with open(f"{MEDIA_PATH}/benchmarks.json",'r') as json_file:
        benchmarks = json.load(json_file)

    x_axis = [benchmark["ebook_num"] for benchmark in benchmarks]
    y_axis_seq = [round(sum(benchmark["seq_timings"])/len(benchmark["seq_timings"]), 3) for benchmark in benchmarks]
    y_axis_par = [round(sum(benchmark["par_timings"])/len(benchmark["par_timings"]), 3) for benchmark in benchmarks]
    y_axis_parV2 = [round(sum(benchmark["par_timings_V2"])/len(benchmark["par_timings_V2"]), 3) for benchmark in benchmarks]
    
    fig, ax = plt.subplots(figsize=(8, 8))

    ax.plot(x_axis, y_axis_seq, label="Sequential",)
    ax.plot(x_axis, y_axis_par, label="Parallel")
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
    ax.set_title('Benchmarks')

    # Combine default and custom legends
    handles, labels = plt.gca().get_legend_handles_labels()
    handles.append(custom_legend)
    labels.append('Speed_up')

    ax.legend(handles=handles, labels=labels, loc='upper left')
    fig.savefig(f"{MEDIA_PATH}/benchmarks.png", dpi=600)
    plt.show()

if __name__ == "__main__":
    main()