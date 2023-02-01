def printProgressBar(iteration: int, total: int, prefix: str = '', suffix: str = '',
                     decimals: int = 1, length: int = 100, fill: str = '█',
                     printEnd: str = "") -> None:
    """
    Call in a loop to create terminal progress bar

    Args:
        iteration: - Required  : current iteration (Int)
        total: - Required  : total iterations (Int)
        prefix: - Optional  : prefix string (Str)
        suffix: - Optional  : suffix string (Str)
        decimals: - Optional  : positive number of decimals in percent complete (Int)
        length: - Optional  : character length of bar (Int)
        fill: - Optional  : bar fill character (Str)
        printEnd: - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()
