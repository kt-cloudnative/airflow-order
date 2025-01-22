from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='PP_CSNC450B',WORKFLOW_ID='98a2d678f8c8479a9a42718b49ac7693', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNC450B-0.0.prd-tz.0.0'
    ,'schedule_interval':'10 00 1 * *'
    ,'start_date': datetime(2024, 10, 21, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('98a2d678f8c8479a9a42718b49ac7693')

    csnc450bJob_vol = []
    csnc450bJob_volMnt = []
    csnc450bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc450bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc450bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc450bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3d5a2488a44d4d72b00a08b112c71d77',
        'volumes': csnc450bJob_vol,
        'volume_mounts': csnc450bJob_volMnt,
        'env_from':csnc450bJob_env,
        'task_id':'csnc450bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csnc450bJob","empNo=82265795", "gubun=3", "date="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('98a2d678f8c8479a9a42718b49ac7693')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnc450bJob,
        Complete
    ]) 

    # authCheck >> csnc450bJob >> Complete
    workflow








