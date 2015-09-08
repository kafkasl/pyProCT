"""
Created on 04/02/2013

@author: victor
"""

import optparse
import threading
import pyproct
import os.path
from pyproct.driver.parameters import ProtocolParameters
from pyproct.driver.observer.observer import Observer
from pyproct.driver.driver import Driver
import pyproct.tools.commonTools as tools
import subprocess
import sys

class CmdLinePrinter(threading.Thread):

    def __init__(self,data_source):
        super(CmdLinePrinter, self).__init__()
        self._stop = threading.Event()
        self.data_source = data_source

    def stop(self):
        self._stop.set()
        self.data_source.notify("Main","Stop","Finished")

    def stopped(self):
        return self._stop.isSet()

    def run(self):
        while not self.stopped():
            self.data_source.wait()
            print self.data_source.get_data()
            self.data_source.clear()

def read_params(json_script):
    try:
        parameters = ProtocolParameters.get_params_from_json(tools.remove_comments(open(json_script).read()))
        parameters["global"]["workspace"]["base"] = os.path.abspath(parameters["global"]["workspace"]["base"])
    except ValueError, e:
        print "Malformed json script."
        print e.message
        exit()
    return parameters


def runcompss_pyproct(json_script):
    control_script = os.path.realpath(json_script)
    main_file = __file__
    classpath = os.path.dirname(main_file)
    subprocess.check_call(["runcompss", "--lang=python", "-d",
        "--classpath=%s" % classpath,
        main_file,
       control_script,
        "pyCOMPSs"])

def mpirun_pyproct(json_script, threads_num):
    if not threads_num:
        import multiprocessing
        threads_num = multiprocessing.cpu_count()
    subprocess.check_call(["mpirun", "-np", "%s" % threads_num,"python", "-m",
        "pyproct.main", json_script, "MPI"])


if __name__ == '__main__':
    parser = optparse.OptionParser(usage='%prog [--mpi] [--print] script', version=pyproct.__version__)

    parser.add_option('--mpi', action="store_true",  dest = "use_mpi", help="Add this flag if you want to use MPI-based scheduling.")
    parser.add_option('--print', action="store_true",  dest = "print_messages", help="Add this flag to print observed messages to stdout.")

    options, args = parser.parse_args()

    if(len(args)==0):
        parser.error("You need to specify the script to be executed.")

    json_script = args[0]
    parameters = read_params(json_script)

    try:
        mode = args[1]
    except Exception, e:
        mode = "main"


    if mode == "main":

        scheduler = parameters["global"]["control"]["scheduler_type"]

        observer = None
        cmd_thread = None
        if scheduler == "MPI/Parallel":
            try:
                threads_num = parameters["global"]["control"]["number_of_processes"]
            except Exception, e:
                threads_num = None
            mpirun_pyproct(json_script, threads_num)
        elif scheduler == "pyCOMPSs":
            runcompss_pyproct(json_script)
        else:
            observer = Observer()
            if options.print_messages:
                cmd_thread = CmdLinePrinter(observer)
                cmd_thread.start()
            Driver(observer).run(parameters)

        if options.print_messages:
            cmd_thread.stop()

    elif mode == "pyCOMPSs":
        from pyproct.driver.compssdriver import CompssDriver
        observer = Observer()
        if options.print_messages:
            cmd_thread = CmdLinePrinter(observer)
            cmd_thread.start()
        CompssDriver(observer).run(parameters)
    elif mode == "MPI":
        from pyproct.driver.mpidriver import MPIDriver
        from pyproct.driver.observer.MPIObserver import MPIObserver
        observer = MPIObserver()
        if options.print_messages:
            cmd_thread = CmdLinePrinter(observer)
            cmd_thread.start()
        MPIDriver(observer).run(parameters)

