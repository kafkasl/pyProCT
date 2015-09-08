"""
Created on 2/9/2014

@author: victor
"""
from pyproct.data.matrix.matrixHandler import MatrixHandler
from pyproct.tools.plugins import PluginHandler
import traceback

class MatrixCalculator(object):

    CALCULATION_METHOD = "None" # Hack that allow us to work with matrix combination.
                                # in skip_list, matrixCalculator has been removed.
                                # TODO: understand why this is necessary

    def __init__(self):
        pass

    @classmethod
    def calculate(cls, data_handler, matrix_params):

        calculator_class = cls.get_calculator(matrix_params)

        try:
            distance_matrix = calculator_class.calculate(data_handler, matrix_params["parameters"])
            import sys
            print "Matrix nbytes: %s bytes" % distance_matrix.get_data().nbytes
            print "Shape %s" % distance_matrix.get_data().shape
            print "Length %s" % len(distance_matrix.get_data())
            print "Type: %s" % type(distance_matrix.get_data()[0])
            print "Size of %s" % sys.getsizeof(distance_matrix.get_data()[0])
            print "Item size %s" % distance_matrix.get_data()[0].itemsize
            print distance_matrix.row_length
        except Exception, e:
            print "[ERROR][Driver::postprocess] Impossible to perform matrix calculation for method: %s"%(calculator_class.CALCULATION_METHOD)
            print "Message: %s"%str(e)
            traceback.print_exc()
            exit()

        return MatrixHandler(distance_matrix, matrix_params)

    @classmethod
    def get_calculator(cls, matrix_params):
        # Get all available calculators
        available_calculators = PluginHandler.get_classes('pyproct.data.matrix',
                                                          selection_keyword = "MatrixCalculator",
                                                          skip_list = ["test", "cases"],
                                                          plugin_name = "matrix")

        # Choose the calculator we need
        calculation_method = matrix_params["method"]
        calculator_classes = filter(lambda x: x.CALCULATION_METHOD == calculation_method, available_calculators)

        if len(calculator_classes) == 0:
            print "[ERROR][MatrixCalculator::calculate] %s is not a registered matrix calculation method."%(calculation_method)
            exit()
        else:
            return calculator_classes[0]
