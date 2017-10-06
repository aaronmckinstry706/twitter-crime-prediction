import importlib
import os.path as path
import sys
import unittest
import zipfile

import jobs

class ZippedTestLoader(unittest.TestLoader):
    def discoverZipped(self, zip_path):
        """This method discovers unittests inside a zip file which is already in the Python
        path."""
        
        if zip_path not in sys.path:
            raise ValueError("zip_path '{}' must be in Python path.".format(zip_path))
        
        try:
            zip_file = zipfile.ZipFile(zip_path)
        except Exception as e:
            raise ValueError('Error when trying to read zip file {path}: {exception}'.format(
                path=zip_path, exception=str(e)))
        
        test_suite = unittest.TestSuite()
        potential_modules = sorted(zip_file.namelist())
        for potential_module in potential_modules:
            if potential_module[-3:] != '.py':
                continue
            parent_directory = path.dirname(potential_module)
            if parent_directory == '' or (parent_directory + '/__init__.py') in potential_modules:
                module_name = potential_module[:-3].replace('/', '.')
                if '.run' in module_name:
                    continue
                module = importlib.import_module(module_name, package=module_name[:module_name.rfind('.')])
                test_suite.addTest(self.loadTestsFromModule(module))
        
        return test_suite

def main():
    test_suite = ZippedTestLoader().discoverZipped(path.dirname(path.dirname(jobs.__file__)))
    unittest.TextTestRunner().run(test_suite)

if __name__ == '__main__':
    main()
