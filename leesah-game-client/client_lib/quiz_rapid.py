import dataclasses
import enum
import json
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from json import JSONDecodeError
from typing import Set

from confluent_kafka import Consumer, Producer

from .config import ENCODING
from .kafka import consumer_config, producer_config
from .schemas import Question as SchemaQuestion, Assessment as SchemaAssessment

ANSWER_LOG_COLOR = "\u001b[34m"
QUESTION_LOG_COLOR = "\u001b[32m"

deserialize = lambda value: json.loads(value.decode(ENCODING))
serialize = lambda value: json.dumps(value).encode(ENCODING)


@dataclass(eq=True, frozen=True)
class Answer:
    questionId: str
    category: str
    teamName: str
    answer: str
    created: str = datetime.now().isoformat()
    messageId: str = str(uuid.uuid4())
    type: str = "ANSWER"


@dataclass
class Question:
    messageId: str
    question: str
    category: str
    created: str
    type: str = "QUESTION"


class AssessmentStatus(enum.Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


@dataclass
class Assessment:
    messageId: str
    category: str
    teamName: str
    questionId: str
    answerId: str
    status: AssessmentStatus
    sign: str
    created: str
    type: str = "ASSESSMENT"


class QuizParticipant(ABC):
    def __init__(self, team_name: str):
        self._outbox = set()
        self._team_name = team_name

    @abstractmethod
    def handle_question(self, question: Question):
        """
        handle questions received from the quiz topic.

        Parameters
        ----------
            question : Question
                question issued by the quizmaster

        """
        pass

    @abstractmethod
    def handle_assessment(self, assessment: Assessment):
        """
        handle assessments received from the quiz topic.

        Parameters
        ----------
            assessment : Assessment
                assessment of an answer by the quizmaster

        """
        pass

    def publish_answer(self, question_id: str, category: str, answer: str):
        """
        publishes an answer to a specific question id with a category

        Parameters
        ----------
            question_id : str
                the messageId of the question to be answered
            category : str
                the category of the question to be answered
            answer : str
                the answer to the question asked

        """
        self.publish(Answer(question_id, category, self._team_name, answer))

    def publish(self, answer: Answer):
        self._outbox.add(answer)

    def messages(self) -> Set[Answer]:
        out = self._outbox
        self._outbox = set()
        return out


class QuizRapid:
    """Mediates messages to and from the quiz rapid on behalf of the quiz participant"""

    def __init__(
        self,
        team_name: str,
        topic: str,
        bootstrap_servers: str,
        consumer_group_id: str,
        auto_commit: bool = True,
        producer=None,
        consumer=None,
        logg_questions=False,
        logg_answers=False,
        short_log_line=False,
        log_ignore_list=None,
    ):
        """
        Constructs all the necessary attributes for the QuizRapid object.

        Parameters
        ----------
            team_name : str
                team name to filter messages on
            topic : str
                topic to produce and consume messages
            bootstrap_servers : str
                kafka host server
            consumer_group_id : str
                the kafka consumer group id to commit offset on
            auto_commit : bool, optional
                auto commit offset for the consumer (default is True)
            producer : Producer, optional
                specify a custom Producer to use (Default None)
            consumer : Consumer, optional
                specify a custom consumer to use (Default None)
            logg_questions : bool, optional
                should the QuizRapid logg incoming questions to the terminal (Default False)
            logg_answers : bool, optional
                should the QuizRapid logg outgoing answers to the terminal (Default False)
            short_log_line : bool, optional
                for enabled loggers, should the output be shortened keeping only essential fields (Default False)
            log_ignore_list : list, optional
                for enabled loggers, should any question categories be ignored (Default None)
        """
        self.running = True
        if consumer is None:
            consumer = Consumer(
                consumer_config(
                    bootstrap_servers, consumer_group_id, auto_commit
                )
            )
            consumer.subscribe([topic])
        if producer is None:
            producer = Producer(producer_config(bootstrap_servers))
        self._name = team_name
        self._producer: Producer = producer
        self._consumer: Consumer = consumer
        self._topic = topic
        self._commit_offset = auto_commit
        self._logg_questions = logg_questions
        self._logg_answers = logg_answers
        self._short_log_line = short_log_line
        self._log_ignore_list = log_ignore_list

    def run(self, participant: QuizParticipant):
        msg = self._consumer.poll(timeout=1)
        if msg is None:
            return
        try:
            msg = deserialize(msg.value())
        except JSONDecodeError as e:
            print(f"error: could not parse message: {msg.value()} error: {e}")
            return
        if SchemaQuestion.is_valid(msg):
            question = Question(
                msg["messageId"],
                msg["question"],
                msg["category"],
                msg["created"],
                msg["type"],
            )
            self._logg_question(question)
            participant.handle_question(question)
        if SchemaAssessment.is_valid(msg) and msg["teamName"] == self._name:
            participant.handle_assessment(
                Assessment(
                    msg["messageId"],
                    msg["category"],
                    msg["teamName"],
                    msg["questionId"],
                    msg["answerId"],
                    AssessmentStatus[msg["status"].upper()],
                    msg["sign"],
                    msg["created"],
                    msg["type"],
                )
            )
        for message in participant.messages():
            self._logg_answer(message)
            self._producer.produce(
                topic=self._topic, value=serialize(dataclasses.asdict(message))
            )
            self._producer.flush(timeout=0.1)
        if self._commit_offset:
            self.commit_offset()

    def commit_offset(self):
        self._consumer.commit()

    def close(self):
        self.running = False
        self._producer.flush()
        self._consumer.close()

    def _logg_question(self, question: Question):
        if self._logg_questions and (
            self._log_ignore_list is None
            or question.category not in self._log_ignore_list
        ):
            question_dict = dataclasses.asdict(question)
            if self._short_log_line:
                question_dict.pop("messageId")
                question_dict.pop("type")
            print(
                "\x1b[33;20m [{}][Q]\x1b[0m {}{}\x1b[0m".format(
                    datetime.now().time().isoformat(),
                    QUESTION_LOG_COLOR,
                    json.dumps(question_dict),
                )
            )

    def _logg_answer(self, answer: Answer):
        if self._logg_answers and (
            self._log_ignore_list is None
            or answer.category not in self._log_ignore_list
        ):
            answer_dict = dataclasses.asdict(answer)
            if self._short_log_line:
                answer_dict.pop("messageId")
                answer_dict.pop("type")
                answer_dict.pop("questionId")
                answer_dict.pop("teamName")
            print(
                "\x1b[33;20m [{}][A]\x1b[0m {}{}\x1b[0m".format(
                    datetime.now().time().isoformat(),
                    ANSWER_LOG_COLOR,
                    json.dumps(answer_dict),
                )
            )
