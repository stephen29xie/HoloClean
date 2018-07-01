from flask import render_template, request, Blueprint, jsonify, redirect, url_for


from holocleanwrapper import HoloCleanWrapper
holoclean = HoloCleanWrapper()

bp = Blueprint('main', __name__)


# get request to get final repaired dataset
@bp.route('/getrepaireddataset', methods=['GET'])
def get_repaired_dataset():

    holoclean.get_repaired_dataset()  # this function should return spark df as json (use jsonify)

    return 'not implemented yet'  # return JSON


@bp.route('/repair', methods=['POST'])
def repair():
    holoclean.repair()

    return '', 204


# home page, query for parameters to initialize holoclean
@bp.route('/', methods=['GET'])
def home():

    return render_template('home.html')


@bp.route('/submitholoparams', methods=['POST'])
def submit_holo_params():

    data = request.get_json()

    holoclean.set_parameters(pruning_threshold1=data['pruning_threshold1'],
                             pruning_threshold2=data['pruning_threshold2'],
                             pruning_clean_breakoff=data['pruning_clean_breakoff'],
                             pruning_dk_breakoff=data['pruning_dk_breakoff'],
                             learning_iterations=data['learning_iterations'],
                             learning_rate=data['learning_rate'],
                             batch_size=data['batch_size'],
                             k_inferred=data['k_inferred'])

    return '', 204


@bp.route('/loadfilepaths', methods=['POST'])
def load_filepaths():

    data = request.get_json()

    datafilepath = data['filepath']
    dcfilepath = data['dcpath']

    holoclean.load_data(dataset_path=datafilepath,
                        denial_constraints_path=dcfilepath)

    return '', 204


@bp.route('/submiterrordetectors', methods=['POST'])
def submiterrordetectors():

    flags = request.get_json()

    holoclean.choose_error_detectors(NullErrorDetectorflag=flags['nullerrordetectorflag'],
                                     DCErrorDetectorflag=flags['dcerrordetectorflag'])

    return '', 204







