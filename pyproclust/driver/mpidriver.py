'''
Created on Mar 26, 2013

@author: victor
'''
from pyproclust.driver.handlers.timerHandler import TimerHandler
from pyproclust.driver.handlers.trajectoryHandler import TrajectoryHandler
from pyproclust.driver.driver import Driver
from mpi4py import MPI
import copy_reg
from pyRMSD.condensedMatrix import CondensedMatrix
import pyRMSD.condensedMatrix
from pyproclust.driver.handlers.matrix.matrixHandler import MatrixHandler

def only_rank_0(rank):
    def wrap(function):
        def wrapped_f(*args):
            if rank == 0:
                function(*args)
            return wrapped_f
        return wrap
 
class MPIDriver(Driver):
    
    def __init__(self, observer):
        super(MPIDriver, self).__init__(observer)
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.workspaceHandler = None
        self.trajectoryHandler = None
        self.matrixHandler = None 
    
    def perform_actions(self, parameters):
        best_clustering = self.get_best_clustering(parameters)
        self.comm.Barrier()
        if self.rank == 0:
            self.postprocess(parameters, best_clustering)  
    
    def run(self, parameters):
        #####################
        # Start timing 
        #####################
        self.timer = TimerHandler()
        self.timer.start("Global")
        
        #####################
        # Workspace Creation 
        #####################
        if self.rank == 0:
            self.create_workspace(parameters)
        self.comm.Barrier()
        self.workspaceHandler = self.comm.bcast(self.workspaceHandler, root=0)
        
        #####################
        # Saving Parameters 
        #####################
        if self.rank == 0:
            self.save_parameters_file(parameters)
        
        #####################
        # Trajectory Loading
        #####################
        self.timer.start("Trajectory Loading")
        self.trajectoryHandler = TrajectoryHandler(parameters["global"], parameters["matrix"]['parameters'], self.observer)
        self.comm.Barrier()
        self.timer.stop("Trajectory Loading")
        
        ##############################
        # Distance Matrix Generation
        ##############################
        if self.rank == 0:
            self.create_matrix(parameters)
        else:
            self.matrixHandler = MatrixHandler(parameters["matrix"])
            self.matrixHandler.distance_matrix = CondensedMatrix([])
        self.comm.Barrier()
        matrix_contents = list(self.comm.bcast(self.matrixHandler.distance_matrix.get_data(), root=0))
        if self.rank != 0:
            self.matrixHandler.distance_matrix = CondensedMatrix(matrix_contents)

        # Adding pickling capabilities to CondensedMatrix
#         CondensedMatrixType = type(CondensedMatrix)
#         def pickle_CondensedMatrix(matrix):
#             return CondensedMatrixType, (list(matrix.get_data()),)
#         
#         copy_reg.constructor(CondensedMatrix)
#         copy_reg.pickle(CondensedMatrix, pickle_CondensedMatrix)
#         
#         #Everybody waits until 0 arrives here (and the matrix is calculated)
#         self.comm.Barrier()
#         self.matrixHandler = self.comm.bcast(self.matrixHandler, root=0) # Give the matrix handler to all processes
#         print self.matrixHandler.distance_matrix.get_data()
        
        ##############################
        # Actions
        ##############################
        self.comm.Barrier()
        self.perform_actions(parameters)
        
        self.timer.stop("Global")
        print self.timer
        