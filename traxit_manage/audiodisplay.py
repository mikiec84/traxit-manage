# -*- coding: utf-8 -*-
"""
Created on Thu Apr 17 12:04:21 2014
@author: fenet
"""

from numpy import floor
from numpy import linspace
from numpy import size


def specgramplot(data, times, freqs, ticks_nb=11, title='', vmin=None, vmax=None):
    """Plots a spectrogram

    Args:
        data (numpy.ndarray): Axis0: freq, axis1: time
        times (numpy.ndarray): Array of the same size as data.axis0
        freqs (numpy.ndarray): Array of the same size as data.axis1
        ticks_nb (int): Number of axes graduations
        title (str): Title

    """
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    stop_f_ind = len(freqs)

    plt.imshow(data[0:stop_f_ind, :], origin='lower', vmin=vmin, vmax=vmax,
               aspect='auto', cmap='copper', interpolation='nearest')
    plt.title(title)
    plt.xlabel('Time')
    plt.ylabel('Frequency')
    plt.yticks(floor(linspace(0, stop_f_ind - 1, ticks_nb)),
               floor(freqs[[int(x)
                            for x in
                            floor(linspace(0, stop_f_ind - 1,
                                           ticks_nb))]]))
    plt.xticks(linspace(0, size(data, 1) - 1, ticks_nb),
               ['%.2f' % x for x in
                times[[int(x) for x in
                       linspace(0, size(data, 1) - 1, ticks_nb)]]])


def binarized_plot(data, times=None, freqs=None, ticks_nb=11,
                   title=''):
    """Plot a binarized array

    Args:
        data (numpy.ndarray): Axis0: freq, axis1: time
        times (numpy.ndarray): Array of the same size as data.axis0
        freqs (numpy.ndarray): Array of the same size as data.axis1
        ticks_nb (int): Number of axes graduations
        title (str): Title for the matplotlib figure

    """
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.imshow(data, aspect='auto', cmap='gist_yarg', origin='lower', interpolation='nearest')

    plt.title(title)
    plt.xlabel('Time')
    plt.ylabel('Frequency')

    if freqs is not None:
        stop_f_ind = len(freqs)
        plt.yticks(floor(linspace(0, stop_f_ind - 1, ticks_nb)),
                   floor(freqs[[int(x)
                                for x in
                                floor(linspace(0, stop_f_ind - 1,
                                               ticks_nb))]]))
    if times is not None:
        plt.xticks(linspace(0, size(data, 1) - 1, ticks_nb),
                   ['%.2f' % x for x in
                    times[[int(x) for x in
                           linspace(0, size(data, 1) - 1, ticks_nb)]]])


def timetime_plot(data, ticks_nb=10, title=''):
    """Plot a time-time diagram

    Args:
        data (numpy.ndarray):   xis0: time, axis1: x, y
        ticks_nb (int): Number of axes graduations
        title (str): Title for the matplotlib figure
    """
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    assert data.shape[1] == 2
    plt.plot(data[:, 0], data[:, 1], '.', markersize=1)

    plt.xlabel('Time in match')
    plt.ylabel('Time in reference')

    plt.xticks(linspace(min(data[:, 0]), max(data[:, 0]), ticks_nb, endpoint=True))
    plt.yticks(linspace(min(data[:, 1]), max(data[:, 1]), ticks_nb, endpoint=True))

    plt.xlim([min(data[:, 0]), max(data[:, 0])])
    plt.ylim([min(data[:, 1]), max(data[:, 1])])
    plt.title(title)
