
import unittest

from api.app import app


class TestLifeMapAPI(unittest.TestCase):

    def setUp(self):
        app.config["TESTING"] = True
        self.client = app.test_client()

    def test_near_requires_coordinates(self):
        response = self.client.get("/businesses/near")
        self.assertEqual(response.status_code, 400)

    def test_near_rejects_invalid_latitude(self):
        response = self.client.get(
            "/businesses/near?lat=120&lon=-75&radius=1000"
        )
        self.assertEqual(response.status_code, 400)

    def test_within_requires_polygon(self):
        response = self.client.post(
            "/businesses/within",
            json={}
        )
        self.assertEqual(response.status_code, 400)

    def test_predict_requires_text(self):
        response = self.client.post(
            "/predict",
            json={}
        )
        self.assertEqual(response.status_code, 400)

    def test_predict_reports_pending_model(self):
        response = self.client.post(
            "/predict",
            json={"text": "The food was excellent"}
        )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.get_json()["status"],
            "pending"
        )


if __name__ == "__main__":
    unittest.main()
