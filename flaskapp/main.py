from flask import render_template, request, Blueprint, jsonify
from holocleanwrapper import HoloCleanWrapper

holoclean = HoloCleanWrapper()

home_bp = Blueprint('main', __name__)

@home_bp.route('/repair', methods=['POST'])
def repair():
    '''
    This endpoint calls repair in HoloClean.
    :return: None
    '''

    data = request.get_json()

    holoclean.repair(initfeaturizer_flag=data['initfeaturizer_flag'],
                     dcfeaturizer_flag=data['dcfeaturizer_flag'],
                     cooccurfeaturizer_flag=data['cooccurfeaturizer_flag'])

    return '', 204  # 204 is a No Content Flask response


# home page, query for parameters to initialize holoclean
@home_bp.route('/', methods=['GET'])
def home():
    '''
    Renders the html for the home page
    '''
    return render_template('home.html')


@home_bp.route('/submitholoparams', methods=['POST'])
def submit_holo_params():
    '''
    This endpoint submits the user-input holoclean parameters and initializes a HoloClean object and Session object
    :return: None
    '''

    data = request.get_json()

    holoclean.set_parameters(pruning_threshold1=data['pruning_threshold1'],
                             pruning_threshold2=data['pruning_threshold2'],
                             pruning_clean_breakoff=data['pruning_clean_breakoff'],
                             pruning_dk_breakoff=data['pruning_dk_breakoff'],
                             learning_iterations=data['learning_iterations'],
                             learning_rate=data['learning_rate'],
                             batch_size=data['batch_size'],
                             k_inferred=data['k_inferred'])

    return '', 204  # 204 is a No Content Flask response


@home_bp.route('/loadfilepaths', methods=['POST'])
def load_filepaths():
    '''
    This endpoint submits the file paths for the data and the denial constraints and the data is read in and loaded
    into the db.
    :return: None
    '''

    data = request.get_json()

    datafilepath = data['filepath']
    dcfilepath = data['dcpath']

    holoclean.load_data(dataset_path=datafilepath,
                        denial_constraints_path=dcfilepath)

    return '', 204  # 204 is a No Content Flask response


@home_bp.route('/submiterrordetectors', methods=['POST'])
def submiterrordetectors():
    '''
    This end point submits the selected error detectors and then performs error detection
    :return: None
    '''

    flags = request.get_json()

    holoclean.choose_error_detectors(NullErrorDetectorflag=flags['nullerrordetectorflag'],
                                     DCErrorDetectorflag=flags['dcerrordetectorflag'])

    return '', 204  # 204 is a No Content Flask response


@home_bp.route('/getrepairedJSON', methods=['GET'])
def getrepairedJSON():
    '''
    This endpoint gets the repaired dataset and returns it in JSON
    :return:
    '''

    return jsonify(holoclean.get_repaired_dataset_psycopg2())


@home_bp.route('/getAttributes', methods=['GET'])
def getColNames():
    '''
    Returns the column names of the repaired dataset in the format required to pass to DataTables.
    :return: JSON response
    '''

    return jsonify(holoclean.get_attributes())

@home_bp.route('/getDirtyCells', methods=['GET'])
def getDirtyCells():
    '''
    Returns the rows of the C_dk table in JSON, indicating which cells are dirty. Eg {'ind': 0, 'attr': Name}
    :return: JSON response
    '''

    return jsonify(holoclean.get_dirty_cells())

@home_bp.route('/getInitValue', methods=['POST', 'GET'])
def getInitValue():
    '''
    Returns the Init value of table cell. This endpoint is called when a user clicks on a repaired cell in the
    repaired dataset. This allows the user to see the initial value before repairing
    :return: JSON response
    '''

    data = request.get_json()

    ind = data['__ind']
    attribute = data['attribute']

    return jsonify(holoclean.get_init_value(ind=ind, attribute=attribute))

@home_bp.route('/getInferredValues', methods=['POST','GET'])
def getInferredValues():

    data = request.get_json()

    ind = data['__ind']
    attribute = data['attribute']

    return jsonify(holoclean.get_inferred_values(ind=ind, attribute=attribute))


