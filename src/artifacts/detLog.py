import sys


def error_detailed(error, error_details=sys):
    _, _, error_tb = error_details.exc_info()
    er_fname = error_tb.tb_frame.f_code.co_filename

    error_msg = f'''\n Error occured in {er_fname} \n at line {error_tb.tb_lineno} \n with message: {str(error)}\n'''
    return error_msg
