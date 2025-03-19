import streamlit as st
import asyncio
from src.creg.creg_information import CREG
from src.upme.upme_information import UPME

if "messages" not in st.session_state:
    st.session_state.messages = []

def get_vector_stores():
    creg = CREG()
    creg_documents = creg.model_resolution_doc()
    if creg_documents:
        creg_vector_store = creg.get_creg_vector_store(creg_documents)
        creg_query_engine = creg.get_query_engine(creg_vector_store)

    upme = UPME()
    upme_documents = upme.model_resolution_doc()
    if upme_documents:
        upme_vector_store = upme.get_upme_vector_store(upme_documents)
        upme_query_engine = upme.get_query_engine(upme_vector_store)
        
    return creg_query_engine, upme_query_engine


async def get_response(prompt: str, engine:str, creg_query_engine, upme_query_engine) -> str:
    if engine == "Resoluciones CREG":
        response = await creg_query_engine.aquery(prompt)
    elif engine == "Resoluciones UPME":
        response = await upme_query_engine.aquery(prompt)
    return response

st.set_page_config(page_title="AI Chatbot del sector energetico", layout="wide")

engine = st.sidebar.selectbox(
    "Selecciona de que quieres obtener información",
    ["Resoluciones CREG", "Resoluciones UPME"]
)

st.title(f"Información sobre el sector energetico de {engine}")

creg_query_engine, upme_query_engine = get_vector_stores()

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.write(message["content"])

if prompt := st.chat_input("¿Que deseas saber?"):
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user"):
        st.write(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Pensando..."):
            response = asyncio.run(
                get_response(prompt, engine, creg_query_engine, upme_query_engine)
            )
            st.write(response.response)

    st.session_state.messages.append({"role": "assistant", "content": response})

if st.sidebar.button("Limpiar Chat"):
    st.session_state.messages = []
    st.rerun()
