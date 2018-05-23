import argparse

from mistpy import metadata_extractor, python_execute_script

def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--module', type=str)
    parser.add_argument('--gateway-port', type=int)
    args = parser.parse_args()
    if args.module == 'metadata_extractor':
        metadata_extractor.metadata_cmd(args)
    elif args.module == 'python_execute_script':
        python_execute_script.execution_cmd(args)
    else:
        print('Unknown module_name: could not execute cmd {}'.format(args.module))

_main()
