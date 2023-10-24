# LEESAH Quiz Client

**System overview**

```bash
┌──────────────┐       ┌──────────────┐
│              │       │              │
│              │       │              │
│  Quizboard   │       │  Quizmaster  │
│              │       │              │
│              │       │              │
└──────────────┘       └──────────────┘
│  ▲                   │  ▲
│  │                   │  │
│  │                   │  │
▼  │                   ▼  │
┌──────────────────────────────────────────────────────────────┐
│                      Kafka                                   │
└──────────────────────────────────────────────────────────────┘
▲    │                           ▲    │
│    │                           │    │
│    ▼                           │    ▼
┌──────────────┐                 ┌──────────────┐
│              │                 │              │
│              │                 │              │
│    Team 1    │      .  .  .    │    Team n    │
│              │                 │              │
│              │                 │              │
└──────────────┘                 └──────────────┘
```

### Prerequirements

- Python ^3.10 🐍
- IDEA of your choice (VS Code/IntelliJ/Atom ...etc) 💻
- A teammate 🐶

## Setup 📝

### 1. To get started, either clone with git or download the repository:

**Clone project with git**

```
git clone https://github.com/navikt/leesah-game-starter.git
```

**Download repository**

```
Click 'Code' on top of this page
Click 'Download ZIP' and download the the repository into the folder you want to store the files in
Unpack ZIP
```

### 2. Setup virtual environment for the project

We recommend to use a virtual environment to install the dependencies. Set one up using

**For macOS/Linux**

```bash
cd leesah-game-starter
python3 -m venv venv
source ./venv/bin/activate
```

**For Windows**

```bash
cd leesah-game-starter
python3 -m venv venv
.\venv\Scripts\activate
```

or see guide [here](https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/#creating-a-virtual-environment)

### 3. Install dependencies

```bash
python3 -m pip install -r requirements.txt
```

### 4. Download Kafka certificates

- In your browser, navigate to link provided by the course hosts.
  - Enter username and password provided by the course crew
- Press ctrl+s or cmd+s to save the file, name it `leesah-creds.yaml` and save it in `certs/` under the project directory

The result should look like this:

```bash
leesah-game-starter
├── certs
│   ├── .gitignore
│   ├── leesah-creds.yaml
```

### 5. Change the application config in main.py

- Set `TEAM_NAME` to your preferred team name 😼 NB! Do not change this during the course!!
- Set `HEX_CODE` to your preferred team color
- Set `QUIZ_TOPIC` to the topic name provided by the course administrators

```python
# Config ##########

TEAM_NAME = "CHANGE ME"
HEX_CODE = "CHANGE ME"
QUIZ_TOPIC = "CHANGE ME"
CONSUMER_GROUP_ID = f"cg-leesah-team-${TEAM_NAME}-1"
```

### 6. Run the application

Run the application from the terminal using:

```bash
python3 leesah-game-client
```

For each change you want to append, run the application again.
If you get a ✅ in your terminal, you're ready to go!👍🏼

## How to play

Your challenge is to implement a QuizParticipant that answers all the questions that are
published by the QuizMaster 🧙. You are free to develop your application as you want but this starter project comes with
some useful boilerplate, so you can focus on the fun part, answering questions! 🎉

The code you need to extend is all located in `./leesah-game-client/main.py` when you run `python3 leesah-game-client`
the `main()` function in `./leesah-game-client/main.py` is executed.

### Main loop

The main loop creates a `QuizRapid` object that runs your `QuizParticipant`.

```python
def main():
    rapid = quiz_rapid.QuizRapid(TEAM_NAME, QUIZ_TOPIC, HOSTED_KAFKA, CONSUMER_GROUP_ID, False)

    try:
        while True:
            rapid.run(MyParticipant())
    except KeyboardInterrupt:
        print("\nstopping...")

```

### QuizParticipant

Again you are free to program the application as you like but the `QuizParticipant` is a handy abstract class you
can extend to get started solving the questions.

There are two methods you need to implement in your own class:

- `handle_question(self, question: quiz_rapid.Question)` is the most important method, it is where you will receive questions to answer.
- (optional) `handle_assessment(self, assessment: Assessment):` Allows you to read assessments the QuizMaster make of your answers.

## Tips and Tricks 💡

**1. Don't be afraid to answer the same question multiple times** _(Except for questions that requires you don't)_

**2. Filter out question categories and write handler functions**

**3. Ask us! We don't bite :)**
