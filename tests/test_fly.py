from fly import FileRecord, FileStructure, FileWrapper


class TestFileWrapper:
    def test_write(self, tmp_path):
        temp_file = tmp_path / 'test_write'
        fw = FileWrapper(temp_file)
        fw.write(10, b'hello')
        assert temp_file.read_bytes() == b'\0' * 10 + b'hello'

    def test_remove_data(self, tmp_path):
        temp_file = tmp_path / 'test_remove_data'
        temp_file.write_bytes(b'\0hello\0')
        fw = FileWrapper(temp_file)
        fw.remove_data(1, 3)
        assert temp_file.read_bytes() == b'\0lo\0'


class TestFileStructure:
    def test_empty(self):
        fs = FileStructure(b'')
        assert fs.files_list == []

    def test_some_files(self):
        fs = FileStructure(b'')
        fr = FileRecord('test_name', 99999)
        fs.files_list.append(fr)
        assert fs.pack() == (
            b'\x01\x00\x00\x00\x09\x00\x00\x00test_name\x00\x00\x00'
            b'\x9f\x86\x01\x00\x00\x00\x00\x00'
        )

    def test_more_files(self):
        fs = FileStructure(b'')
        fr1 = FileRecord('test_name', 99999)
        fr2 = FileRecord('test_name2', 88888)
        fs.files_list.extend([fr1, fr2])
        assert fs.pack() == (
            b'\x02\x00\x00\x00\x09\x00\x00\x00test_name\x00\x00\x00'
            b'\x9f\x86\x01\x00\x00\x00\x00\x00'
            b'\x0a\x00\x00\x00test_name2\x00\x00'
            b'\x38\x5b\x01\x00\x00\x00\x00\x00'
        )
