import matplotlib.pyplot as plt
import json


def main():

    with open("./output/benchmarks_backup.json",'r') as json_file:
        benchmarks = json.load(json_file)
        # print(benchmarks)

    x_axis = [benchmark["ebook_num"] for benchmark in benchmarks]
    y_axis_seq = [round(sum(benchmark["seq_timings"])/len(benchmark["seq_timings"]), 3) for benchmark in benchmarks]
    y_axis_par = [round(sum(benchmark["par_timings"])/len(benchmark["par_timings"]), 3) for benchmark in benchmarks]
    y_axis_parV2 = [round(sum(benchmark["par_timings_V2"])/len(benchmark["par_timings_V2"]), 3) for benchmark in benchmarks]
    
    fig, ax = plt.subplots(figsize=(8, 8))

    ax.plot(x_axis, y_axis_seq, label="Sequential")
    ax.plot(x_axis, y_axis_par, label="Parallel")
    ax.plot(x_axis, y_axis_parV2, label="Parallel V2")
    

    # Add labels and title
    ax.set_xlabel('Number of ebook analysed')
    ax.set_ylabel('Time [s]')
    ax.set_title('Benchmarks')

    ax.legend()
    plt.show()

if __name__ == "__main__":
    main()