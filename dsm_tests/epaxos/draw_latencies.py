import numpy as np
import matplotlib.pyplot as plt
import seaborn

from dsm_tests.epaxos.zeromq import clients

def myplot(x, y, nb=32, xsize=500, ysize=500):
    xmin = np.min(x)
    xmax = np.max(x)
    ymin = np.min(y)
    ymax = np.max(y)

    x0 = (xmin+xmax)/2.
    y0 = (ymin+ymax)/2.

    pos = np.zeros([3, len(x)])
    pos[0,:] = x
    pos[1,:] = y
    w = np.ones(len(x))

    P = sph.Particles(pos, w, nb=nb)
    S = sph.Scene(P)
    S.update_camera(r='infinity', x=x0, y=y0, z=0,
                    xsize=xsize, ysize=ysize)
    R = sph.Render(S)
    R.set_logscale()
    img = R.get_image()
    extent = R.get_extent()
    for i, j in zip(xrange(4), [x0,x0,y0,y0]):
        extent[i] += j
    # print extent
    return img, extent

def main():
    arrays = [np.load(f'latencies-{peer_id}.npy') for peer_id in clients]

    fig = plt.figure(1, figsize=(10,10))

    for idx, arr in enumerate(arrays):
        ax1 = fig.add_subplot(3, 4, idx + 1)
        xs = np.arange(len(arr))
        arr[arr > 0.02] = 0.02

        ax1.plot(xs,arr,'k.', markersize=1)
    fig.tight_layout()
    plt.show()


if __name__ == '__main__':
    main()