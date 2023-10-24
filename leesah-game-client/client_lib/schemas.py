from schema import Schema, Or

Question = Schema(
    {
        "messageId": str,
        "type": "QUESTION",
        "category": str,
        "question": str,
        "created": str,
    }
)

Answer = Schema(
    {
        "messageId": str,
        "questionId": str,
        "type": "ANSWER",
        "teamName": str,
        "answer": str,
        "created": str,
    }
)

Assessment = Schema(
    {
        "messageId": str,
        "questionId": str,
        "answerId": str,
        "type": "ASSESSMENT",
        "category": str,
        "teamName": str,
        "status": Or("SUCCESS", "FAILURE"),
        "created": str,
        "sign": str,
    }
)
