import time
import os

from utils import download_bbox_from_s3

OUTPUT_DIRECTORY = "../data/download/"
BUCKET = "s3://us-west-2.opendata.source.coop/roorda-tudelft/public-trees-in-nl/"

def main():
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    # nl_trees_2
    # nl_trees_no_hilbert
    # BBox next to border of delft and the hague, 3138pts:  bbox.xmin > 4.36314 AND bbox.xmax < 4.38221 AND bbox.ymin > 52.026 AND bbox.ymax < 52.03792
    # BBox nexy to AMS, haarlem, velsen: 52,38729°  4,6458° : 52,4513°  4,74816°
    iterations = 10
    avg = 0
    for t in range(iterations):
        t1 = time.time()
        download_bbox_from_s3(BUCKET + "nl_trees_2", OUTPUT_DIRECTORY + "sorted.parquet", 4.6458, 4.74816, 52.38729, 52.4513)
        t2 = time.time()
        diff = t2 - t1
        avg += diff
        print(diff)
    avg /= iterations
    print("avg sorted:", avg)

    avg = 0
    for t in range(iterations):
        t1 = time.time()
        download_bbox_from_s3(BUCKET + "nl_trees_no_hilbert", OUTPUT_DIRECTORY + "unsorted.parquet", 4.6458, 4.74816, 52.38729, 52.4513)
        t2 = time.time()
        diff = t2 - t1
        avg += diff
        print(diff)
    avg /= iterations
    print("avg unsorted:", avg)

if __name__ == '__main__':
    main()