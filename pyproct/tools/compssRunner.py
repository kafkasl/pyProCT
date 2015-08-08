'''
Created on 16/08/2012

@author: victor
'''

class CompssTask(object):
    from pycompss.api.task import task
    """
    Representation of a task.
    """
    def __init__(self, function, parameters, name, matrix_data, kwargs, description = ""):
        """
        Creates a Task object.
        @param function: A callable object that will perform the real work of the task.
        @param name: The name of the task (an identifier).
        @param kwargs: Parameters for the callable.
        # @param description: A short description of what the task does. Can be empty.
        """
        self.function = function
        self.name = name
        self.parameters = parameters
        self.kwargs = kwargs
        self.result = None
        self.matrix_data = matrix_data
        self.kwargs['algorithm'].condensed_matrix = None
        # print "ALGORITHM\n%s\n" % kwargs.get('algorithm')
        # print "PARAMS\n%s\n" % parameters


    @task(returns=tuple)
    def task_run(self):
        print "STARTING TASK"

        """
        Runs the task's associated callable and returns its result.
        @return: The result of the callable execution.
        """
        # from pyRMSD.condensedMatrix import CondensedMatrix
        # if module_exists("CondensedMatrix"):
        #     print "Module exists"
        # else:
        #     print "Module not exists"

        # args = zip(self.kwargs.keys(), self.kwargs.values())

        from pyRMSD.condensedMatrix import CondensedMatrix
        from pyproct.data.matrix.matrixHandler import MatrixHandler

        # print "Task_run::Argument[algorithm]: \n%s" % self.kwargs["algorithm"]
        print "Task_run::Arguments : %s\n" % self.kwargs["algorithm_kwargs"].keys()


        distance_matrix = CondensedMatrix(self.matrix_data)
        # # print "Correctly redone CM: %s" % (cm_copy.get_data() == self.matrix_data)
        # print "ALG: %s" % self.kwargs['algorithm']
        # self.kwargs['algorithm'].__init__(self)

        self.kwargs["algorithm"].condensed_matrix = distance_matrix
        # print "Matrix -------------------\n %s" % distance_matrix.get_data()
        # # self.kwargs["algorithm"].condensed_matrix = matrix_handler
        # # self.kwargs["algorithm"].total_elements = matrix_handler.row_length
        # # self.kwargs["algorithm"].class_list = [0]*matrix_handler.row_length

        # # print "Task_run::Matrix_handler: \n%s" % matrix_handler
        self.result = self.function(**(self.kwargs))

        self.kwargs["algorithm"].condensed_matrix = None


        return self.result # (clustering_id, clustering)