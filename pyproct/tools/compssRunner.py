'''
Created on 16/08/2012

@author: victor
'''

class CompssTask(object):
    from pycompss.api.task import task
    """
    Representation of a task.
    """
    def __init__(self, function, name, kwargs, description = ""):
        """
        Creates a Task object.
        @param function: A callable object that will perform the real work of the task.
        @param name: The name of the task (an identifier).
        @param kkwargs: Parameters for the callable.
        # @param description: A short description of what the task does. Can be empty.
        """
        kwargs.get('algorithm').pickle_condensed_matrix()
        self.function = function
        self.name = name
        self.kwargs = kwargs
        self.result = None
        print "\n%s\n" % kwargs.get('algorithm')


    @task(returns=tuple)
    def task_run(self):
        print "STARTING TASK"

        # from pyRMSD.condensedMatrix import CondensedMatrix
        # if module_exists("CondensedMatrix"):
        #     print "Module exists"
        # else:
        #     print "Module not exists"

        # args = zip(self.kwargs.keys(), self.kwargs.values())
        print "Task arguments : %s\n" % self.kwargs.keys()
        """
        Runs the task's associated callable and returns its result.
        @return: The result of the callable execution.
        """
        self.result = self.function(**(self.kwargs))
        print "RESULTAT\n "
        print type(self.result)
        return self.result

class CompssRunner(object):
    """
    Base scheduling class. It ensures that no task is executed before its dependencies (without building a
    dependency tree).
    It allows to define some functions that will be executed when the scheduler reaches some strategic points.
    TODO: In all scheduler types a dependencies must be checked to avoid cycles for instance.
    """

    def __init__(self, functions = {}):
        """
        Constructor. Initializes needed variables.

        @param fucntions: A dictionary containing 3 possible keys. Each key defines another dictionary of two
        entries ('function' and 'kwargs') with a callable and its arguments. The possible keys are:
            'task_started' -> Were an action performed after each task is called is defined.
            'task_ended' -> Defines the action performed when a task is finished.
            'scheduling_started' -> Defines the action performed when the scheduler starts to run tasks.
            'scheduling_ended' -> Defines the action performed when the scheduler has finished to run all tasks.
        """
        self.functions = functions
        self.tasks = {}
        self.dependencies = {}
        self.not_completed = []
        self.finished = []
        self.results = []

    def function_exec(self, function_type, info = None):
        """
        Execute one of the predefined functions if defined.

        @param function_type: Type of the function to check and run (proper types should be 'task_start','task_end'
        and 'scheduling_end', each defining 'function' and 'kwargs' entries.

        """
        if function_type in self.functions:
            self.functions[function_type]['kwargs']['info'] = info
            self.functions[function_type]['function'](**(self.functions[function_type]['kwargs']))

    def run(self):
        """
        Runs all the tasks in a way that tasks are not executed before their dependencies are
        cleared.

        @return: An array with the results of task calculations.
        """


        for task in self.tasks.itervalues():
            # self.function_exec('task_started', {"task_name":task.name})
            print "Calling task_run()"
            self.results.append(task.task_run())
            # self.function_exec('task_ended', {"task_name":task.name, "finished":len(self.finished)})

        # self.function_exec('scheduling_ended')

        return self.results


    def add_task(self, task_name, dependencies, target_function, function_kwargs, description):
        """
        Adds a task to the scheduler. The task will be executed along with the other tasks when the 'run' function is called.

        @param task_name:
        @param dependencies: A list with the task_names of the tasks that must be fulfilled before executing this other task.
        Example of dependencies dictionary:
            {"task_C":["dep_task_A", "dep_task_B"]}
            This dependencies dict. means that task C cannot be run until task B and A are cleared.
        @param target_function: The function executed by this task.
        @param function_kwargs: Its arguments.
        @param description: A brief description of the task.
        """

        if not task_name in self.tasks:
            task = CompssTask( name = task_name, description = description, function = target_function, kwargs=function_kwargs)
            task.description = description
            self.tasks[task_name] = task
        else:
            print "[Error pyCOMPSs::add_task] Task %s already exists. Task name must be unique."%task_name
            exit()
