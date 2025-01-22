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
                , WORKFLOW_NAME='TC_CBOL033B',WORKFLOW_ID='1085959d959d46d3953ab0160073934b', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-TC_CBOL033B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 26, 18, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1085959d959d46d3953ab0160073934b')

    cbol033bJob_vol = []
    cbol033bJob_volMnt = []
    cbol033bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbol033bJob_env.extend([getICISConfigMap('icis-oder-trmncust-batch-mng-configmap'), getICISSecret('icis-oder-trmncust-batch-mng-secret'), getICISConfigMap('icis-oder-trmncust-batch-configmap'), getICISSecret('icis-oder-trmncust-batch-secret')])
    cbol033bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol033bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'dc599eeb731b402088a59aa387d27edd',
        'volumes': cbol033bJob_vol,
        'volume_mounts': cbol033bJob_volMnt,
        'env_from':cbol033bJob_env,
        'task_id':'cbol033bJob',
        'image':'/icis/icis-oder-trmncust-batch:0.7.1.1',
        'arguments':["--job.name=cbol033bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('1085959d959d46d3953ab0160073934b')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbol033bJob,
        Complete
    ]) 

    # authCheck >> cbol033bJob >> Complete
    workflow








