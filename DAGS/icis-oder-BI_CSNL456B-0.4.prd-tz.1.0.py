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
                , WORKFLOW_NAME='BI_CSNL456B',WORKFLOW_ID='132ba2df1a8e4b6abbe40d176b5bfec4', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-BI_CSNL456B-0.4.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 11, 5, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('132ba2df1a8e4b6abbe40d176b5bfec4')

    csnl456bJob_vol = []
    csnl456bJob_volMnt = []
    csnl456bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnl456bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnl456bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnl456bJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    csnl456bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnl456bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e483dd4603b64101ae244f7ce545e0af',
        'volumes': csnl456bJob_vol,
        'volume_mounts': csnl456bJob_volMnt,
        'env_from':csnl456bJob_env,
        'task_id':'csnl456bJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.5',
        'arguments':["--job.name=csnl456bJob", "workDate=20110527", "toDate=20170530", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('132ba2df1a8e4b6abbe40d176b5bfec4')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnl456bJob,
        Complete
    ]) 

    # authCheck >> csnl456bJob >> Complete
    workflow








