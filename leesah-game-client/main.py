import base64
import requests
from client_lib import quiz_rapid
from client_lib.config import HOSTED_KAFKA

# LEESAH QUIZ GAME CLIENT

# 1. Set `TEAM_NAME` to your preferred team name
# 2. Set `HEX_CODE` to your preferred team color
# 3. Set `QUIZ_TOPIC` to the topic name provided by the course administrators
# 4. Make sure you have downloaded and unpacked the credential files in the
#    `certs/` directory

# Config ######################################################################

TEAM_NAME = "Holters"
HEX_CODE = "000000"
QUIZ_TOPIC = "leesah-quiz-abakus-1"
CONSUMER_GROUP_ID = f"cg-leesah-team-${TEAM_NAME}-1"

# #############################################################################


class MyParticipant(quiz_rapid.QuizParticipant):
    def __init__(self) -> None:
        super().__init__(TEAM_NAME)
        self.balance = 0
        self.previous_questions: set[str] = set()

    def handle_question(self, question: quiz_rapid.Question) -> None:
        reply = ""
        match question.category:
            case "register_team":
                self.handle_register_team(question)
                return
            case "ping-pong":
                reply = "pong"
            case "arithmetic":
                substrings = question.question.split()
                number_1 = int(substrings[0])
                number_2 = int(substrings[2])

                match substrings[1]:
                    case "+":
                        reply = str(number_1 + number_2)
                    case "-":
                        reply = str(number_1 - number_2)
                    case "*":
                        reply = str(number_1 * number_2)
                    case "/":
                        reply = str(number_1 // number_2)
            case "NAV":
                if question.question.endswith(
                    "Hvor finner man informasjon om rekruttering til NAV IT?"
                ):
                    reply = "detsombetyrnoe.no"
                elif question.question.endswith(
                    "Hva heter applikasjonsplattformen til NAV?"
                ):
                    reply = "nais"
                elif question.question.endswith(
                    "Hva heter NAV-direkt\u00f8ren?"
                ) or question.question.endswith("Hva heter NAV-direktøren?"):
                    reply = "Hans Christian Holte"
                elif question.question.endswith("Hvor har vi kontor?"):
                    reply = "Oslo"
                elif question.question.endswith(
                    "Hva heter designsystemet v\u00e5rt?"
                ) or question.question.endswith(
                    "Hva heter designsystemet vårt?"
                ):
                    reply = "Aksel"
                elif question.question.endswith(
                    "Hvor mye er 1G per 1. mai 2023?"
                ):
                    reply = "118620"
            case "is-a-prime":
                number = int(question.question.split()[-1])
                for i in range(2, number):
                    if number % i == 0:
                        reply = str(False)
                        break
                else:
                    reply = str(True)
            case "transactions":
                substrings = question.question.split()
                action = substrings[0]
                amount = int(substrings[1])

                match action:
                    case "INNSKUDD":
                        self.balance += amount
                    case "UTTREKK":
                        self.balance -= amount

                reply = str(self.balance)
            case "base64":
                reply = base64.b64decode(question.question.split()[-1]).decode(
                    "utf-8"
                )
            case "grunnbelop":
                date = question.question.split()[-1]
                reply = str(
                    requests.get(
                        f"https://g.nav.no/api/v1/grunnbeloep?dato={date}",
                        timeout=100,
                    ).json()["grunnbeloep"]
                )
            case "min-max":
                numbers = [
                    int(number)
                    for number in question.question[
                        question.question.index("[") + 1 : -1
                    ].split(",")
                ]

                match question.question.split()[0]:
                    case "LAVESTE":
                        reply = str(min(numbers))
                    case "HØYESTE" | "HOYESTE":
                        reply = str(max(numbers))
            case "deduplication":
                if question.question in self.previous_questions:
                    return

                self.previous_questions.add(question.question)
                reply = "you won't dupe me!"

        self.publish_answer(
            question_id=question.messageId,
            category=question.category,
            answer=reply,
        )

    def handle_assessment(self, assessment: quiz_rapid.Assessment) -> None:
        pass

    # ------------------------------------------------------- Question handlers

    def handle_register_team(self, question: quiz_rapid.Question) -> None:
        self.publish_answer(
            question_id=question.messageId,
            category=question.category,
            answer=HEX_CODE,
        )


def main() -> tuple[MyParticipant, quiz_rapid.QuizRapid]:
    rapid = quiz_rapid.QuizRapid(
        team_name=TEAM_NAME,
        topic=QUIZ_TOPIC,
        bootstrap_servers=HOSTED_KAFKA,
        consumer_group_id=CONSUMER_GROUP_ID,
        auto_commit=False,  # Bare skru på denne om du vet hva du driver med :)
        logg_questions=True,  # Logg spørsmålene appen mottar
        logg_answers=True,  # Logg svarene appen sender
        short_log_line=True,  # Logg bare en forkortet versjon av meldingene
        log_ignore_list=[
            "arithmetic",
            "ping-pong",
            "team-registration",
            "is-a-prime",
            "transactions",
            "base64",
            "grunnbelop",
        ],  # Liste med spørsmålskategorier loggingen skal ignorere
    )
    return MyParticipant(), rapid
