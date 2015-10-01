"""
Created on 16/07/2014

@author: victor
"""
import traceback
from pyproct.tools.plugins import PluginHandler
from pyproct.driver.driver import Driver
from pyproct.driver.workspace.workspaceHandler import WorkspaceHandler
from pyproct.data.dataDriver import DataDriver
from pyRMSD.condensedMatrix import CondensedMatrix
from pyproct.data.matrix.matrixHandler import MatrixHandler


class PostprocessingDriver(object):
    from pycompss.api.task import task

    def __init__(self):
        pass


    @task()
    def postprocess(self, clustering, parameters,
                           data_handler, workspace_handler, matrix_data,
                           generated_files, postprocessing_action_class, postprocessing_parameters):
      matrix_handler = MatrixHandler(CondensedMatrix(matrix_data.view('float64')), parameters["data"]["matrix"])
      if postprocessing_action_class.KEYWORD in postprocessing_parameters:
          try:
              postprocessing_action_class().run(clustering,
                                        postprocessing_parameters[postprocessing_action_class.KEYWORD],
                                        data_handler,
                                        workspace_handler,
                                        matrix_handler,
                                        generated_files)
          except Exception, e:
              print "[ERROR][Driver::postprocess] Impossible to perform '%s' postprocessing action."%(postprocessing_action_class.KEYWORD)
              print "Message: %s"%str(e)
              traceback.print_exc()


    def run(self, clustering, parameters, data_handler, matrix_data, generated_files):
      with WorkspaceHandler(parameters["global"]["workspace"], None) as workspace_handler:
        postprocessing_parameters = parameters["postprocess"]
        if "data" in parameters:
            data_handler = DataDriver.load_data(parameters["data"])
            matrix_handler = MatrixHandler(CondensedMatrix(matrix_data.view('float64')), parameters["data"]["matrix"])

            available_action_classes = PluginHandler.get_classes('pyproct.postprocess.actions',
                                                                    "PostAction",
                                                                    ["test","confSpaceComparison"],
                                                                    "postprocess")

            for postprocessing_action_class in available_action_classes:
              self.postprocess(clustering, parameters,
                           data_handler, workspace_handler, matrix_data,
                           generated_files, postprocessing_action_class, postprocessing_parameters)