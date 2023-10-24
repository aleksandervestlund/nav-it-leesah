import base64
import requests
from client_lib import quiz_rapid
from client_lib.config import HOSTED_KAFKA

# LEESAH QUIZ GAME CLIENT

# 1. Set `TEAM_NAME` to your preferred team name
# 2. Set `HEX_CODE` to your preferred team color
# 3. Set `QUIZ_TOPIC` to the topic name provided by the course administrators
# 4. Make sure you have downloaded and unpacked the credential files in the certs/ dir

# Config ######################################################################

TEAM_NAME = "Holters"
HEX_CODE = "000000"
QUIZ_TOPIC = "leesah-quiz-abakus-1"
CONSUMER_GROUP_ID = f"cg-leesah-team-${TEAM_NAME}-1"

# #############################################################################


class MyParticipant(quiz_rapid.QuizParticipant):
    def __init__(self):
        super().__init__(TEAM_NAME)
        self.saldo = 0
        self.previous_questions: set[str] = set()

    def handle_question(self, question: quiz_rapid.Question):
        svar = ""
        match question.category:
            case "register_team":
                self.handle_register_team(question)
            case "ping-pong":
                svar = "pong"
            case "arithmetic":
                ting = question.question.split()
                tall_1 = int(ting[0])
                tall_2 = int(ting[2])

                match ting[1]:
                    case "+":
                        svar = str(tall_1 + tall_2)
                    case "-":
                        svar = str(tall_1 - tall_2)
                    case "*":
                        svar = str(tall_1 * tall_2)
                    case "/":
                        svar = str(tall_1 // tall_2)
            case "NAV":
                if question.question.endswith(
                    "man informasjon om rekruttering til NAV IT?"
                ):
                    svar = "detsombetyrnoe.no"
                elif question.question.endswith(
                    "applikasjonsplattformen til NAV?"
                ):
                    svar = "nais"
                elif question.question.endswith(
                    "Hva heter NAV-direkt\u00f8ren?"
                ):
                    svar = "Hans Christian Holte"
                elif question.question.endswith("Hvor har vi kontor?"):
                    svar = "Oslo"
                elif question.question.endswith(
                    "Hva heter designsystemet v\u00e5rt?"
                ):
                    svar = "Aksel"
                elif question.question.endswith(
                    "Hvor mye er 1G per 1. mai 2023?"
                ):
                    svar = "118620"
            case "is-a-prime":
                tall = int(question.question.split()[-1])
                for i in range(2, tall):
                    if tall % i == 0:
                        svar = str(False)
                        break
                else:
                    svar = str(True)
            case "transactions":
                ting = question.question.split()
                handling = ting[0]
                beloep = int(ting[1])

                match handling:
                    case "INNSKUDD":
                        self.saldo += beloep
                    case "UTTREKK":
                        self.saldo -= beloep

                svar = str(self.saldo)
            case "base64":
                svar = base64.b64decode(question.question.split()[-1]).decode(
                    "utf-8"
                )
            case "grunnbelop":
                dato = question.question.split()[-1]
                svar = str(
                    requests.get(
                        f"https://g.nav.no/api/v1/grunnbeloep?dato={dato}",
                        timeout=100,
                    ).json()["grunnbeloep"]
                )
            case "min-max":
                idx = question.question.index("[") + 1

                match question.question.split()[0]:
                    case "LAVESTE":
                        svar = str(
                            min(
                                int(i)
                                for i in question.question[idx:-1].split(",")
                            )
                        )
                    case "HØYESTE" | "HOYESTE":
                        svar = str(
                            max(
                                int(i)
                                for i in question.question[idx:-1].split(",")
                            )
                        )
            case "deduplication":
                if question.question in self.previous_questions:
                    return
                self.previous_questions.add(question.question)
                svar = "you won't dupe me!"

        self.publish_answer(
            question_id=question.messageId,
            category=question.category,
            answer=svar,
        )

    def handle_assessment(self, assessment: quiz_rapid.Assessment):
        pass

    # ---------------------------------------------------------------------------- Question handlers

    def handle_register_team(self, question: quiz_rapid.Question):
        self.publish_answer(
            question_id=question.messageId,
            category=question.category,
            answer=HEX_CODE,
        )


def main():
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
