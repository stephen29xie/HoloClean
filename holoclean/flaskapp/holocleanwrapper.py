from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.sql_dcerrordetector import SqlDCErrorDetection
from holoclean.errordetection.sql_nullerrordetector import SqlnullErrorDetection


class HoloCleanWrapper:
    '''
    This class is a wrapper for HoloClean, to interact with our Flask application.

    '''


    def __init__(self):

        self.pruning_threshold1 = None
        self.pruning_threshold2 = None
        self.pruning_clean_breakoff = None
        self.pruning_dk_breakoff = None
        self.learning_iterations = None
        self.learning_rate = None
        self.batch_size = None
        self.k_inferred = None

        self.data_path = None
        self.dc_path = None

        self.holo_obj = None
        self.session = None

        self.logger = None

        self.Dcdetector = None
        self.Nulldetector = None

        # state variables
        self.load_holo_params = 0  # set to 1 after setting input params and initializing HoloClean object
        self.load_data_path = 0  # set to 1 after loading data file path
        self.load_dc_path = 0  # set to 1 after loading denial constraint file path
        self.select_error_detectors = 0  # set to 1 after selecting error detectors
        self.repaired = 0  # set to 1 after repairing


    def set_parameters(self,
                       pruning_threshold1,
                       pruning_threshold2,
                       pruning_clean_breakoff,
                       pruning_dk_breakoff,
                       learning_iterations,
                       learning_rate,
                       batch_size,
                       k_inferred):

        self.pruning_threshold1 = pruning_threshold1
        self.pruning_threshold2 = pruning_threshold2
        self.pruning_clean_breakoff = pruning_clean_breakoff
        self.pruning_dk_breakoff = pruning_dk_breakoff
        self.learning_iterations = learning_iterations
        self.learning_rate = learning_rate
        self.batch_size = batch_size
        self.k_inferred = k_inferred

        self.init_holo_obj()
        self.load_holo_params = 1

        self.init_session()

    # holoclean pipeline methods (initializing objects, running methods like error detection, repairing, ...)
    def init_holo_obj(self):

        self.holo_obj = HoloClean(
            holoclean_path="..",  # path to holoclean package
            verbose=True,
            # to limit possible values for training data
            pruning_threshold1=self.pruning_threshold1,
            # to limit possible values for training data to less than k values
            pruning_clean_breakoff=self.pruning_clean_breakoff,
            # to limit possible values for dirty data (applied after
            # Threshold 1)
            pruning_threshold2=self.pruning_threshold2,
            # to limit possible values for dirty data to less than k values
            pruning_dk_breakoff=self.pruning_dk_breakoff,
            # learning parameters
            learning_iterations=self.learning_iterations,
            learning_rate=self.learning_rate,
            batch_size=self.batch_size,
            # number of inferred values
            k_inferred=self.k_inferred
        )


    def init_session(self):

        self.session = Session(self.holo_obj)


    def load_data(self, dataset_path, denial_constraints_path):
        '''
        :param dataset_path: string literal of path to dataset csv file
        :param denial_constraints_path: string literal of path to denial constraints txt file
        :return: None
        '''

        self.data_path = dataset_path
        self.session.load_data(self.data_path)
        self.load_data_path = 1

        self.dc_path = denial_constraints_path
        self.session.load_denial_constraints(self.dc_path)
        self.load_dc_path = 1

    def choose_error_detectors(self, NullErrorDetectorflag, DCErrorDetectorflag):

        detector_list = []

        if NullErrorDetectorflag:
            self.Nulldetector = SqlnullErrorDetection(self.session)
            detector_list.append(self.Nulldetector)
        if DCErrorDetectorflag:
            self.Dcdetector = SqlDCErrorDetection(self.session)
            detector_list.append(self.Dcdetector)

        self.session.detect_errors(detector_list)

        self.select_error_detectors = 1


    def repair(self):

        # if self.load_holo_params != 1 or self.load_data_path != 1 or self.load_dc_path != 1 \
        #     or self.choose_error_detectors != 1:


        self.session.repair()

        self.repaired = 1

    def get_repaired_dataset(self):

        pass  # this should return spark df as JSON




