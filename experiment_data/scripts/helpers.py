import os.path


def get_raw_file_path(workload, datasize):
    return "raw_files/" + workload + "_" + datasize + "_dedup" + ".csv"


def create_csv_file(folder_path, csv_file_name):
    os.makedirs(folder_path, exist_ok=True)
    csv_file_path = folder_path + "/" + csv_file_name
    return csv_file_path


def write_to_csv(csv_content, folder_path, csv_file_name):
    os.makedirs(folder_path, exist_ok=True)
    csv_file_path = folder_path + "/" + csv_file_name
    print('writing to csv... ' + csv_file_path)
    with open(csv_file_path, 'w') as write_obj:
        write_obj.write(csv_content)


def write_to_figure(fig, folder_path, fig_name):
    os.makedirs(folder_path, exist_ok=True)
    fig_path = folder_path + "/" + fig_name
    print('writing to figure... ' + fig_path)
    fig.savefig(fig_path)


def set_figure_size(fig, w, h):
    l = fig.subplotpars.left
    r = fig.subplotpars.right
    t = fig.subplotpars.top
    b = fig.subplotpars.bottom
    figw = float(w)/(r-l)
    figh = float(h)/(t-b)
    fig.set_size_inches(figw, figh)


def annotate_boxplot(bp, ax):
    for element in ['whiskers',  'medians']:
        for line in bp[element]:
            # Get the position of the element. y is the label you want
            (x_l, y), (x_r, _) = line.get_xydata()
            if not np.isnan(y):
                if element == 'whiskers':
                    x_line_center = x_r + 0.2
                    y_line_center = y
                else:
                    x_line_center = x_r - 0.6
                    y_line_center = y
                ax.text(x_line_center, y_line_center,  # Position
                        '%.2f' % y,  # Value (3f = 3 decimal float)
                        verticalalignment='center',  # Centered vertically with line
                        fontsize=7)


def calc_ratio(numerator, denominator, num_decimals):
    return round(float(numerator) / float(denominator), num_decimals)
