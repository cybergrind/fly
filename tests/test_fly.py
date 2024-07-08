from fly import FileWrapper


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
