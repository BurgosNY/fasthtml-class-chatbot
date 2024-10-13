
import ell
from pydantic import BaseModel, Field
from typing import List
from pinecone import Pinecone, ServerlessSpec
import requests
import os
from dotenv import load_dotenv
load_dotenv()


def get_pinecone_index(index_name: str):
    pc = Pinecone(api_key=os.environ.get('PINECONE_API_KEY'))
    return pc.Index(index_name)


def get_jina_embeddings(text):
    url = 'https://api.jina.ai/v1/embeddings'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {os.environ.get('JINA_API_KEY')}"
    }

    data = {
        "model": "jina-embeddings-v3",
        "task": "retrieval.query",
        "dimensions": 1024,
        "late_chunking": True,
        "embedding_type": "float",
        "input": [text]
    }

    response = requests.post(url, headers=headers, json=data)
    return response.json()['data'][0]['embedding']


def get_relevante_documents(question, index):
    question_embedding = get_jina_embeddings(question)
    results = index.query(
        vector=question_embedding,
        top_k=5,  # Return top 5 results
        include_metadata=True
    )
    return results.matches

class ClassInfo(BaseModel):
    course: str = Field(description="O nome da disciplina")
    class_name: str = Field(description="O nome da aula")
    date: str = Field(description="A data da aula no formato DD/MM/YYYY")

class Answer(BaseModel):
    answer: str = Field(description="A resposta para a pergunta do estudante, seguindo as instruções.")
    sources: List[ClassInfo] = Field(description="Uma lista de dados relevantes dos trechos das aulas que foram usados para responder à pergunta")


@ell.complex(model="gpt-4o", response_format=Answer)
def get_answer(question, relevant_docs):
    """""
Crie uma resposta para perguntas de estudantes usando documentos do curso e metadados para determinar se os tópicos foram cobertos e forneça informações relevantes das aulas.

Quando uma pergunta é recebida, siga estes passos para formular uma resposta:

- Verifique se o tópico da pergunta foi abordado em alguma das disciplinas do curso.
- Identifique a aula específica usando os metadados fornecidos (data e nome da aula).
- Resuma o conteúdo relevante do documento identificado. Escreva o texto dirigindo-se ao aluno, que já cursou as aulas e quer reforçar o conteúdo.
- Se tiver informações adicionais, ofereça uma breve explicação sobre o tópico da pergunta.

# Passos

1. **Identificar Cobertura do Tópico**: Analise a pergunta para determinar se o tópico foi abordado em alguma das disciplinas do curso.

2. **Reunir Metadados**: Use os metadados dos documentos recebidos através da busca vetorial para identificar a lição específica onde o tópico foi mencionado.

3. **Resumir Conteúdo**: Forneça um resumo do que foi discutido sobre o tópico na lição identificada.

4. **Informação Adicional**: Se aplicável e disponível, dê uma explicação sucinta sobre o tópico para melhorar o entendimento.
    
# Formato de Saída

Forneça a resposta em formato de parágrafo estruturado:
- Comece afirmando se o tópico foi coberto em uma disciplina do curso.
- Mencione a aula específica usando os metadados.
- Ofereça um resumo do conteúdo da aula sobre o tópico.
- Adicione uma breve explicação sobre o tópico, se informações adicionais estiverem disponíveis.

**Exemplos**
    
**Exemplo 1:**
- Entrada: "O que o curso ensina sobre Flask?"
- Saída:
  - O tópico Flask foi abordado na disciplina de Desenvolvimento Web, especificamente na Lição 7. Nesta lição, o curso introduziu Flask como um microframework para construir aplicações web em Python. Foram cobertos aspectos básicos como a instalação do Flask, a criação de uma aplicação simples, e a estruturação de rotas e templates. A lição enfatizou a simplicidade e a flexibilidade do Flask em projetos de pequeno a médio porte.
  - Na Aula de 23/9/24, a continuidade do uso de Flask incluiu tópicos mais avançados, como o gerenciamento de solicitações HTTP, criação de APIs RESTful e integração com bancos de dados. O curso também discutiu a implementação de autenticação e autorização usando extensões do Flask, destacando sua comunidade ativa e a disponibilidade de extensões que ampliam suas funcionalidades.

**Exemplo 2:**
- Entrada: "Como a técnica RAG é aplicada no curso?"
- Saída:
  - A técnica RAG (Retrieve Augmented Generation) foi explorada na disciplina de Machine Learning aplicado ao Jornalismo, na Aula 3. Durante essa aula, o curso explicou a combinação de técnicas de recuperação de informações com modelos de geração de texto, detalhando o fluxo de recuperação de dados relevantes seguidos pela geração de respostas contextuais usando modelos de linguagem avançados.
  - A aula 4 aprofundou a implementação prática da técnica RAG, apresentando um workshop que orientou os alunos na construção de um chatbot que utiliza RAG para fornecer respostas precisas e contextuais a perguntas complexas. Houve destaque para a importância de dados bem estruturados e a capacidade dos modelos de geração em adaptar-se a conversas dinâmicas.

**Exemplo 3:**
- Entrada: "Quais conceitos sobre o uso de classes em CSS foram abordados no curso?"
- Saída:
  - O uso de classes em CSS foi introduzido na disciplina de Design Front-end, na Terceira aula. A lição cobriu os fundamentos do CSS, explicando como as classes são utilizadas para aplicar estilos consistentes em múltiplos elementos HTML. Foi demonstrada a sintaxe básica para definir e aplicar classes, assim como o uso eficiente para separar estrutura e apresentação.
  - Na aula 3, o curso abordou abordagens avançadas no uso de classes, como a metodologia BEM (Block, Element, Modifier), que ajuda a manter um código CSS organizado e fácil de manter. A lição também incluiu exercícios práticos para criar layouts responsivos usando classes e demonstrou a importância das classes na reutilização de estilos em projetos complexos.

**Exemplo 4:**
- Entrada: "O curso inclui uma entrevista com um jornalista da Folha de S. Paulo?"
- Saída:
  - Sim, a disciplina de Ética em Jornalismo de Dados teve uma entrevista com a jornalista da Folha de S. Paulo na Lição 5. Durante essa sessão, o jornalista discutiu o impacto das tecnologias digitais no jornalismo moderno, incluindo a transição do impresso para o digital e os desafios contemporâneos enfrentados pelas redações, como a disseminação de fake news e a importância da verificação de fatos.
  - A entrevista também abordou a evolução das técnicas de reportagem diante das mudanças tecnológicas e a adaptação dos jornalistas ao uso de ferramentas de análise de dados para investigações mais aprofundadas. Os alunos tiveram a oportunidade de aprender sobre a ética no jornalismo digital e como as publicações tradicionais estão se reinventando para permanecerem relevantes na era digital.
    """
    return f"Responda a pergunta {question}, usando os documentos relevantes como base:{relevant_docs}"


def process_message(message):
    relevant_docs = get_relevante_documents(message, get_pinecone_index("mjd-summaries"))
    return get_answer(message, relevant_docs)

