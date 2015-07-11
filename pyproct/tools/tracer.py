from multiprocessing import Lock
import subprocess
import os


class Tracer(object):
    mutex = Lock()

    @classmethod
    def emit(cls, slot, val, eType):
        with cls.mutex:
            command = "/bin/bash emit.sh %s %s %s" % (slot, val, eType)
            print "Emitting: %s" % command
            return_val = subprocess.check_output(command, shell=True)
            print "Result %s" % return_val