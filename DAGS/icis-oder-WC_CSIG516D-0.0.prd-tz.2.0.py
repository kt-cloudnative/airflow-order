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
                , WORKFLOW_NAME='WC_CSIG516D',WORKFLOW_ID='bd2d925e979a4d2aa3ea5d722437f227', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CSIG516D-0.0.prd-tz.2.0'
    ,'schedule_interval':' */3 * * * *'
    ,'start_date': datetime(2024, 9, 6, 8, 40, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('bd2d925e979a4d2aa3ea5d722437f227')

    csig516dJob_vol = []
    csig516dJob_volMnt = []
    csig516dJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csig516dJob_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csig516dJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csig516dJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd1a63c9ff9e841268442519b82838a8d',
        'volumes': csig516dJob_vol,
        'volume_mounts': csig516dJob_volMnt,
        'env_from':csig516dJob_env,
        'task_id':'csig516dJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.6',
        'arguments':["--job.name=csig516dJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('bd2d925e979a4d2aa3ea5d722437f227')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csig516dJob,
        Complete
    ]) 

    # authCheck >> csig516dJob >> Complete
    workflow








