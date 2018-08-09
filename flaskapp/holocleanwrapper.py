from holoclean.holoclean import HoloClean, Session
from holoclean.errordetection.sql_dcerrordetector import SqlDCErrorDetection
from holoclean.errordetection.sql_nullerrordetector import SqlnullErrorDetection
import psycopg2.extras


class HoloCleanWrapper:
    '''
    This class is a wrapper for HoloClean, to interact with the Flask application.

    All of the methods called by this wrapper are from HoloClean. So understanding HoloClean will be neccesary to
    understand this class.

    These class methods are accessed by the flask app's endpoints. There is not many comments/documentation on this
    class because all methods call holoclean methods directly with none or little modification.

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

    def set_parameters(self,
                       pruning_threshold1,
                       pruning_threshold2,
                       pruning_clean_breakoff,
                       pruning_dk_breakoff,
                       learning_iterations,
                       learning_rate,
                       batch_size,
                       k_inferred):
        '''
        These are some of the HoloClean parameters needed to init holoclean. Namely the same ones in the tutorial
        notebook. This should be changed if the front end is changed to prompt more/less parameters.
        :return:
        '''

        self.pruning_threshold1 = pruning_threshold1
        self.pruning_threshold2 = pruning_threshold2
        self.pruning_clean_breakoff = pruning_clean_breakoff
        self.pruning_dk_breakoff = pruning_dk_breakoff
        self.learning_iterations = learning_iterations
        self.learning_rate = learning_rate
        self.batch_size = batch_size
        self.k_inferred = k_inferred

        self.init_holo_obj()

        self.init_session()


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

        self.dc_path = denial_constraints_path
        self.session.load_denial_constraints(self.dc_path)

    def choose_error_detectors(self, NullErrorDetectorflag, DCErrorDetectorflag):

        detector_list = []

        if NullErrorDetectorflag:
            self.Nulldetector = SqlnullErrorDetection(self.session)
            detector_list.append(self.Nulldetector)
        if DCErrorDetectorflag:
            self.Dcdetector = SqlDCErrorDetection(self.session)
            detector_list.append(self.Dcdetector)

        self.session.detect_errors(detector_list)


    def repair(self, initfeaturizer_flag, dcfeaturizer_flag, cooccurfeaturizer_flag):

        self.session.repair(initfeaturizer_flag=initfeaturizer_flag,
                            dcfeaturizer_flag=dcfeaturizer_flag,
                            cooccurfeaturizer_flag=cooccurfeaturizer_flag)

    def get_repaired_dataset_psycopg2(self):
        '''
        This function gets the existing connection from psycopg2 and creates a new cursor to execute the given query
        and return the query set.
        :return:
        '''

        query = 'SELECT * FROM ' + self.session.dataset.table_specific_name('Repaired_dataset_with_ind')

        cur, conn = self.holo_obj.dataengine.get_db_backend()

        # create new server_side cursor
        ss_cur = conn.cursor(name='server_side_cur_repaired', cursor_factory=psycopg2.extras.RealDictCursor)
        ss_cur.itersize = 2000

        ss_cur.execute(query)
        data = ss_cur.fetchall()

        # close cursor
        ss_cur.close()

        assert (ss_cur.closed)

        return data

    def get_attributes(self):
        '''
        Gets the attributes/columns of the dataset in the form of: [{"data":attr, "title":attr}]
        This format is needed for DataTables
        :return:
        '''

        attributes = self.session.dataset.get_schema('Init')

        attributes = [{"data": attribute.lower(), "title": attribute.lower()} for attribute in attributes]

        print(attributes)

        return attributes

    def get_dirty_cells(self):
        '''
        This function gets the existing connection from psycopg2 and creates a new cursor to execute the given query
        and return the query set.
        :return:
        '''

        query = 'SELECT * FROM ' + self.session.dataset.table_specific_name('C_dk')

        cur, conn = self.holo_obj.dataengine.get_db_backend()

        # create new server_side cursor
        ss_cur = conn.cursor(name='server_side_cur_dirty', cursor_factory=psycopg2.extras.RealDictCursor)
        ss_cur.itersize = 2000

        ss_cur.execute(query)
        data = ss_cur.fetchall()

        # close cursor
        ss_cur.close()

        assert(ss_cur.closed)

        return data

    def get_clean_cells(self):
        '''
        This function gets the existing connection from psycopg2 and creates a new cursor to execute the given query
        and return the query set.
        :return:
        '''

        query = 'SELECT * FROM ' + self.session.dataset.table_specific_name('C_clean')

        cur, conn = self.holo_obj.dataengine.get_db_backend()

        # create new server_side cursor
        ss_cur = conn.cursor(name='server_side_cur_clean', cursor_factory=psycopg2.extras.RealDictCursor)
        ss_cur.itersize = 2000

        ss_cur.execute(query)
        data = ss_cur.fetchall()

        # close cursor
        ss_cur.close()

        assert (ss_cur.closed)

        return data

    def get_init_value(self, ind, attribute):
        '''
        This function gets the existing connection from psycopg2 and creates a new cursor to execute the given query
        and return the query set.
        :return:
        '''

        query = 'SELECT ' + attribute + ' FROM ' + self.session.dataset.table_specific_name('Init') + \
                ' WHERE __ind = ' + str(ind)

        cur, conn = self.holo_obj.dataengine.get_db_backend()

        cur.execute(query)

        data = cur.fetchall()

        return data

    def get_inferred_values(self, ind, attribute):
        '''
        This function gets the existing connection from psycopg2 and creates a new cursor to execute the given query
        and return the query set.

        More specifically, this function gets the inferred values of a specific cell, identified by its __ind and
        attribute. If there are no inferred values (it is a clean cell), it will return a message.
        :return:
        '''

        query = 'SELECT attr_val, probability FROM ' + self.session.dataset.table_specific_name('Inferred_values') + \
                ' WHERE tid = ' + str(ind) + ' AND LOWER(attr_name) = \'' + attribute + '\''

        cur, conn = self.holo_obj.dataengine.get_db_backend()

        cur.execute(query)

        if cur.rowcount == 0:
            return 'Clean Cell ... No Inferred Values'
        else:
            data = cur.fetchall()
            return data








