import requests
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain.chat_models import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
import time
import streamlit as st
import json


#load_dotenv()



if(st.session_state.get("authentication_status") is None):
    st.session_state.authentication_status=False




def submit():
            llm = ChatOpenAI(temperature=0.5,openai_api_key=st.session_state.openai_key, model_name="gpt-3.5-turbo")
            st.session_state.llm=llm
            st.session_state.authentication_status=True
            st.sidebar.success("You have successfully authenticated", icon='✅')  

 
  
with st.sidebar:
      
    with st.form("flink_form"):
                st.title('Configurtion details')
                
                openaiKey =st.text_input("OpenAI API Key", type="password", key="openai_key")
                confluentApiKey = st.text_input("Confluent Flink API Key", key="confluentApiKey")
                confluentApiSecret= st.text_input("Confluent Flink API Secret", type="password", key="confluentApiSecret")
                confluentEnvironment= st.text_input("Confluent Environment ID", key="confluentEnvironment")
                confluentOrg=st.text_input("Confluent Organization ID", key="confluentOrg")
                confluentPool=st.text_input("Confluent Flink Compute Pool", key="confluentPool")
                confluentPrincipal=st.text_input("Confluent Flink Principal", key="confluentPrincipal")
                confluentCatalog=st.text_input("Confluent Flink Environment Name", key="confluentCatalog")
                confluentDatabase=st.text_input("Confluent Kafka Cluster Name(Flink DB)", key="confluentDatabase")
                confluentCloudProvider=st.selectbox("Cloud provider", ["azure", "aws", "gcp"], key="confluentCloudProvider")
                confluentCloudRegion=st.text_input("Cloud provider region", key="confluentCloudRegion")
                submit_button = st.form_submit_button("Submit", on_click=submit)                  
        
              
             
st.image("confluent-logo.png", width=50)       
if(st.session_state.get("authentication_status") is False):
     st.title("Flink SQL- A chatbot for Flink SQL")
     st.subheader("Please fill in the form to authenticate")
else:
    st.title("Flink SQL- A chatbot for Flink SQL")
    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "assistant", "content": "I am a chatbot which can analyze and query your flink tables?"}]




template = """Based on the table schema below, write a flink SQL query that would answer the user's question, limit the query to 10 rows if the query does not have it.Creation timestamp is represented by a hidden row $rowtime which is of datatype TIMESTAMP_LTZ(3). So if you want to compare it to a string you are sending , you will have to cast the string to TIMESTAMP_LTZ(3) . Don't use columns in where condition if the table does not have the column. Dont use asias names which are keywords like count.  
{schema}

Question: {question}
SQL Query:"""


fulltemplate = """Based on the table schema below, write a flink SQL query that would answer the user's question, limit the query to 10 rows if the question does not have it.Creation timestamp is represented by a hidden row $rowtime which has a precision of 3. Don't use columns in where condition if the table does not have the column.  Then from the response create a HTML table with the Columns has headers and data as rows(only if the data is not none), also include the SQL query after the HTML Table:
{schema}

Question: {question}
SQL Query: {query}
Data: {response}"""

sqlprompt = ChatPromptTemplate.from_template(template)
fullprompt = ChatPromptTemplate.from_template(fulltemplate)


def getFlinkStatementsURL():
    return f"https://flink.{confluentCloudRegion}.{confluentCloudProvider}.confluent.cloud/sql/v1beta1/organizations/{confluentOrg}/environments/{confluentEnvironment}/statements"

@st.cache_data(show_spinner=False)
def get_schema():


    requests.delete(getFlinkStatementsURL()+"/show-tables",auth=(confluentApiKey, confluentApiSecret))
        
    result = requests.post(getFlinkStatementsURL(),
                json= {
    'name': 'show-tables',
    'organization_id': confluentOrg,
    'environment_id': confluentEnvironment,
    'spec': {
    'statement': 'show tables',
    'properties': {'sql.current-database': 'cluster_0', 'sql.current-catalog': 'rahul-env'},
    'compute_pool_id': confluentPool,
    'principal': confluentPrincipal,
    'stopped': False
    }}, auth=(confluentApiKey, confluentApiSecret))

    time.sleep(10)
    results  = requests.get(getFlinkStatementsURL()+"/show-tables/results",auth=(confluentApiKey, confluentApiSecret))
    resultTables = [ x.get('row')[0] for x in results.json().get('results').get('data')]

    schema = ""
    
    for table in resultTables:
        tableNameinURL=str.replace(table, '.', '-').replace('_','-')
        result = requests.post(getFlinkStatementsURL(),
                json= {
                    'name': 'show-create-'+tableNameinURL,
                    'organization_id': confluentOrg,
                    'environment_id': confluentEnvironment,
                    'spec': {
                        'statement': f'show create table `{table}`',
                        'properties': {'sql.current-database': 'cluster_0', 'sql.current-catalog': 'rahul-env'},
                        'compute_pool_id': confluentPool,
                        'principal': confluentPrincipal,
                        'stopped': False
                    }
                    }, auth=(confluentApiKey, confluentApiSecret))
        if(result.json().get('status_code')!=200):
            
            time.sleep(10)
            resultschema = requests.get(getFlinkStatementsURL()+"/show-create-"+tableNameinURL+"/results",auth=(confluentApiKey, confluentApiSecret))
            schema+=resultschema.json().get('results').get('data')[0].get('row')[0]
        else:
             print(result.json())

        requests.delete(getFlinkStatementsURL()+"/show-create-"+tableNameinURL,auth=(confluentApiKey, confluentApiSecret))
    print(schema)
    
    
    
    return schema



def run_query(query):
    try:
        requests.delete(getFlinkStatementsURL()+"/user-flink-query",auth=(confluentApiKey, confluentApiSecret))
        resultofCreateQuery = requests.post(getFlinkStatementsURL(),
                json= {
                        'name': 'user-flink-query',
                        'organization_id': confluentOrg,
                        'environment_id': confluentEnvironment,
                        'spec': {
                        'statement': query,
                        'properties': {'sql.current-database': 'cluster_0', 'sql.current-catalog': 'rahul-env'},
                        'compute_pool_id': confluentPool,
                        'principal': confluentPrincipal,
                        'stopped': False
                        }}, auth=(confluentApiKey, confluentApiSecret))
        print(resultofCreateQuery.json())
        time.sleep(10)
        results =requests.get(getFlinkStatementsURL()+"/user-flink-query",auth=(confluentApiKey, confluentApiSecret))
        columns=results.json().get('status').get('result_schema').get('columns')

        results =requests.get(getFlinkStatementsURL()+"/user-flink-query/results?page_size=100",auth=(confluentApiKey,confluentApiSecret))
        data=results.json().get('results').get('data')
            
        if data is None or len(data) == 0:
            next=results.json().get('metadata').get('next')
            results =requests.get(next,auth=(confluentApiKey, confluentApiSecret))
            data=results.json().get('results').get('data')
        dataRows=[]    
        for dataRow in data:
            op = dataRow.get('op')
            if op is 0:
                    dataRows.append(dataRow.get('row'))
            elif op is 1:
                    dataRows.pop()
            elif op is  2:
                    dataRows.append(dataRow.get('row'))        
            elif op is  3:
                    dataRows.pop()
            else:
                    dataRows.append(dataRow.get('row'))
                
        
        return {"columns":columns, "data":dataRows}
    except Exception as e:
        print(f"got execption for query {query}")
        print(e)
        return {"columns":None, "data":None}    




if(st.session_state.get("authentication_status") is True):  
    llm = st.session_state.llm
    sql_chain = (
        sqlprompt
        | llm.bind(stop=["\nSQLResult:"])
        | StrOutputParser()
    )

    full_chain = (
        RunnablePassthrough.assign(query=sql_chain).assign(
            response=lambda vars: run_query(vars["query"]),
        )
        | fullprompt
        | llm
    )
    with st.chat_message("assistant"):
            with st.spinner("Analyzing schema..."):
                schema=get_schema()
                st.session_state.messages.append({"role": "assistant", "content": "I finished analyzing your tables, you can now ask me any question"})
                st.markdown("I finished analyzing your tables, you can now ask me any question")
    st.session_state["promptDisabled"] = schema is None

    if prompt:=st.chat_input("Enter your question",  disabled=st.session_state.promptDisabled):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)

    if st.session_state.messages[-1]["role"] != "assistant":
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                llmresult =full_chain.invoke({"schema":schema, "question":prompt})
                placeholder = st.empty()
                placeholder.markdown("Following is the response for your query.</br>"+llmresult.content,  unsafe_allow_html=True)
            
        st.session_state.messages.append({"role": "assistant", "content": "Following is the response for your query, this is a continously running streaming query , so the results below get updated every 10 seconds. If you want to stop ask a new question or close the window.</br>"+llmresult.content})
    
    