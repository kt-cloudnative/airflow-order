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
                , WORKFLOW_NAME='BI_CSAB910B',WORKFLOW_ID='f1e069415fa94b83b4453f9430e8dbec', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-BI_CSAB910B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 17, 13, 45, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f1e069415fa94b83b4453f9430e8dbec')

    csab910bJob_vol = []
    csab910bJob_volMnt = []
    csab910bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csab910bJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    csab910bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csab910bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8eac300882c64a3ebb1d13943f4f8739',
        'volumes': csab910bJob_vol,
        'volume_mounts': csab910bJob_volMnt,
        'env_from':csab910bJob_env,
        'task_id':'csab910bJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.4',
        'arguments':["--job.name=csab910bJob", "date="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f1e069415fa94b83b4453f9430e8dbec')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csab910bJob,
        Complete
    ]) 

    # authCheck >> csab910bJob >> Complete
    workflow








