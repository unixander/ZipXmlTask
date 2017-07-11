import os
import errno
import getopt
import sys
import zipfile
import random
from lxml import etree
from io import BytesIO
import uuid
import string
import time
import csv
from multiprocessing import Manager
from functools import partial
import concurrent.futures


def profiler(fn):
    def inner(*args, **kwargs):
        start_time = time.time()
        result = fn(*args, **kwargs)
        print ('{0}: {1:.4f} s'.format(fn.__name__, time.time() - start_time))
        return result
    return inner


class TestTask(object):

    def __init__(self, root_path, zip_prefix='test_', xml_prefix='test_',
                 absolute=False, create=True, **kwargs):
        self._root_path = self._init_path(root_path, absolute=absolute, create=create)
        self._zip_pattern = '{}{{}}.zip'.format(zip_prefix)
        self._xml_pattern = '{}{{}}.xml'.format(xml_prefix)

    def _get_zipfilename(self, index):
        return self._zip_pattern.format(index)

    def _get_fullpath(self, path):
        return os.path.join(self._root_path, path)

    def _get_xmlfilename(self, index):
        return self._xml_pattern.format(index)

    def _init_path(self, path, absolute, create):
        # Init root path
        if not absolute:
            current_path = os.path.dirname(os.path.realpath(__file__))
            path = os.path.join(current_path, path)
        if os.path.exists(path):
            if not os.path.isdir(path):
                raise Exception('{} exists and is not a directory.'.format(path))
        else:
            if create:
                try:
                    os.makedirs(path)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        pass  # Directory exists
                    else:
                        raise Exception('Cannot create directory {}'.format(path))
            else:
                raise Exception('Directory {} does not exist'.format(path))
        return path

    def _generate_xml(self):
        # Generate single xml file content
        # Root element <root>
        root = etree.Element('root')

        # <var name='id' value='random unique string value'/>
        id_var = etree.Element('var')
        id_var.set('name', 'id')
        id_var.set('value', str(uuid.uuid4()))
        root.append(id_var)

        # <var name='level' value='random number 1-100'/>
        level_var = etree.Element('var')
        level_var.set('name', 'level')
        level_var.set('value', str(random.randint(1, 100)))
        root.append(level_var)

        # <objects>
        objects = etree.Element('objects')

        # Nested objects (quantity 1-10)
        for i in range(0, random.randint(1, 10)):
            # <object name='random string value'/>
            obj = etree.Element('object')
            obj.set('name', ''.join(random.SystemRandom().choice(string.ascii_letters)))
            objects.append(obj)

        root.append(objects)
        return etree.tostring(root, pretty_print=True)

    def _generate_archive(self, index, xml_qty):
        # Generate zip archive with xml files
        input_buffer = BytesIO()
        with zipfile.ZipFile(input_buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
            for i in range(0, xml_qty):
                zf.writestr(self._get_xmlfilename(i), self._generate_xml())

        with open(self._get_fullpath(self._get_zipfilename(index)), 'wb') as f:
            f.write(input_buffer.getvalue())

    @profiler
    def generate_archives(self, qty, xml_qty):
        # Generate zip files in separate processes
        partial_generate = partial(self._generate_archive, xml_qty=xml_qty)
        with concurrent.futures.ProcessPoolExecutor() as executor:
            executor.map(partial_generate, range(0, qty))

    def _process_zip_result(self, zip_file):
        # Open zip file and handle its content
        level_rows = []
        objects_rows = []
        with zipfile.ZipFile(self._get_fullpath(zip_file), 'r') as zf:
            # Get list of files in zip
            for xml_name in zf.namelist():
                if not xml_name.endswith('.xml'):
                    continue
                # Open each file and parse it
                with zf.open(xml_name) as xml_file:
                    document = etree.parse(xml_file)
                    level = None
                    current_id = None
                    for var in document.xpath('//var'):
                        if var.attrib['name'] == 'level':
                            level = var.attrib['value']
                        elif var.attrib['name'] == 'id':
                            current_id = var.attrib['value']
                    level_rows.append([current_id, level])

                    for obj in document.xpath('//objects/object'):
                        objects_rows.append([current_id, obj.attrib['name']])
        return level_rows, objects_rows

    def _prepare_csv(self, output_prefix, delete_existing=True):
        # Prepare empty result files

        level_filepath = self._get_fullpath('{0}levels.csv'.format(output_prefix))
        objects_filepath = self._get_fullpath('{0}objects.csv'.format(output_prefix))
        for filepath in (level_filepath, objects_filepath):
            if os.path.exists(filepath):
                if delete_existing:
                    os.remove(filepath)
                else:
                    raise Exception('File {} already exists.'.format(filepath))

        # Prepare base file
        with open(level_filepath, 'w') as level_file, open(objects_filepath, 'w') as object_file:
            level_writer = csv.writer(level_file, delimiter=',')
            objects_writer = csv.writer(object_file, delimiter=',')
            level_writer.writerow(['id', 'level'])
            objects_writer.writerow(['id', 'object_name'])

        return level_filepath, objects_filepath

    @profiler
    def process_to_csv(self, output_prefix='test_', delete_existing=True):
        # Gather data from zip files and write to xml files
        # output_prefix - prefix for csv file names
        # delete_existing - indicates whether we should delete existing files

        # Prepare empty files and get their paths
        level_filepath, objects_filepath = self._prepare_csv(output_prefix, delete_existing)

        # List all zip files in root path directory
        zip_files = (zip_file for zip_file in os.listdir(self._root_path) if zip_file.endswith('.zip'))

        # Run process files in multiple processes to get more CPU usage
        with concurrent.futures.ProcessPoolExecutor() as executor:
            with open(level_filepath, 'w') as level_file, open(objects_filepath, 'w') as object_file:
                level_writer = csv.writer(level_file, delimiter=',')
                objects_writer = csv.writer(object_file, delimiter=',')

                # Get results from processes
                for level_rows, object_rows in executor.map(self._process_zip_result, zip_files):
                    # Write levels data
                    for row in level_rows:
                        level_writer.writerow(row)

                    # Write objects data
                    for row in object_rows:
                        objects_writer.writerow(row)


def execute(zip_number, xml_number, **kwargs):
    print ('Generating files...')
    test_task = TestTask(**kwargs)
    test_task.generate_archives(zip_number, xml_number)
    print ('Compiling csv files...')
    test_task.process_to_csv()
    print ('Finished')


def parse_args():
    zip_number = 50
    xml_number = 100
    arguments = sys.argv[1:]
    kwargs = {'root_path': 'test_folder'}
    try:
        opts, args = getopt.getopt(arguments, 'd:z:x:a:f:', ['dest=', 'zip-prefix=', 'xml-prefix=', 'help', 'zip-archives=', 'xml-files='])
    except getopt.GetoptError:
        pass
    else:
        for opt, arg in opts:
            if opt in ('-d', '--dest'):
                kwargs['root_path'] = arg
            elif opt in ('-z', '--zip-prefix'):
                kwargs['zip_prefix'] = arg
            elif opt in ('-x', '--xml-prefix'):
                kwargs['xml_prefix'] = arg
            elif opt in ('-a', '--zip-archives'):
                try:
                    zip_number = int(arg)
                    if zip_number < 0:
                        raise ValueError()
                except ValueError:
                    print ('zip_number should be positive integer value.')
                    return None, None, {}
            elif opt in ('-f', '--xml-files'):
                try:
                    xml_number = int(arg)
                    if xml_number <= 0:
                        raise ValueError()
                except ValueError:
                    print ('xml_number should be positive integer value.')
                    return None, None, {}
            elif opt in ('-h', '--help'):
                print ('python test_task.py -d test_folder -z zip_ -x xml_ -a 50 -f 100')
                return None, None, {}
    return zip_number, xml_number, kwargs


if __name__ == '__main__':
    zip_number, xml_number, kwargs = parse_args()
    if zip_number and xml_number:
        execute(zip_number, xml_number, **kwargs)
