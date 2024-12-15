import unittest 

from fake_data import generate_mock_data


class TestGenerateMockValue(unittest.TestCase):

    def test_integer_type(self):
        value = generate_mock_value(IntegerType())
        self.assertIsInstance(value, int)
        self.assertGreaterEqual(value, 1)
        self.assertLessEqual(value, 1000)

    def test_long_type(self):
        value = generate_mock_value(LongType())
        self.assertIsInstance(value, int)
        self.assertGreaterEqual(value, 1)
        self.assertLessEqual(value, 1000000)

    def test_float_type(self):
        value = generate_mock_value(FloatType())
        self.assertIsInstance(value, float)
        self.assertGreaterEqual(value, 1.0)
        self.assertLessEqual(value, 1000.0)

    def test_double_type(self):
        value = generate_mock_value(DoubleType())
        self.assertIsInstance(value, float)
        self.assertGreaterEqual(value, 1.0)
        self.assertLessEqual(value, 1000000.0)

    def test_string_type(self):
        value = generate_mock_value(StringType())
        self.assertIsInstance(value, str)
        self.assertGreater(len(value), 0)

    def test_boolean_type(self):
        value = generate_mock_value(BooleanType())
        self.assertIsInstance(value, bool)

    def test_date_type(self):
        value = generate_mock_value(DateType())
        self.assertIsInstance(value, date)

    def test_timestamp_type(self):
        value = generate_mock_value(TimestampType())
        self.assertIsInstance(value, datetime)

    def test_fallback(self):
        # Testar um tipo não mapeado
        value = generate_mock_value(Decimal)
        self.assertIsInstance(value, str)  # Fallback retorna string

# Executar os testes
if __name__ == "__main__":
    print("teste")
    unittest.main()