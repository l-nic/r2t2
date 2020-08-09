import os
import threading
import time
import datetime

num_treelets = 4
run_name = None

def launch_main_thread():
    os.system("./split-rays ../treelets/ 1 2>&1 > ../logs/" + run_name + "/rootlog")

def launch_treelet_thread(treelet):
    os.system("./ray-client ../treelets/ 1 " + str(treelet) + " 2>&1 > ../logs/" + run_name + "/treelog" + str(treelet))

def main():
    os.chdir("../../build")
    os.system("make")
    global run_name
    run_name = "ray_tests_" + str(datetime.datetime.now().strftime("%Y_%m_%d-%H_%M_%S"))
    os.system("mkdir " + "../logs/" + run_name)
    os.system("rm -f ../logs/recent")
    os.system("ln -s ../logs/" + run_name + " ../logs/recent")
    all_theads = []
    root_thread = threading.Thread(target=launch_main_thread)
    all_theads.append(root_thread)
    root_thread.start()
    time.sleep(1)
    for i in range(num_treelets):
        t = threading.Thread(target=launch_treelet_thread, args=(i, ))
        t.start()
        all_theads.append(t)
    for t in all_theads:
        t.join()



if __name__ == "__main__":
    main()