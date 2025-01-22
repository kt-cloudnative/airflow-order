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
                , WORKFLOW_NAME='PP_CSNG951B',WORKFLOW_ID='5ba8c08f06d546b9ac2638570ed9657e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG951B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 17, 0, 00, tzinfo=local_tz)
    ,'end_date': datetime(2024, 11, 1, 22, 0, 00, tzinfo=local_tz)
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('5ba8c08f06d546b9ac2638570ed9657e')

    csng951bJob_vol = []
    csng951bJob_volMnt = []
    csng951bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng951bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng951bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng951bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd4912e412b9642d3bfb2bfa380950f15',
        'volumes': csng951bJob_vol,
        'volume_mounts': csng951bJob_volMnt,
        'env_from':csng951bJob_env,
        'task_id':'csng951bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.45',
        'arguments':["--job.name=csng951bJob", "endTranDate=${YYYYMM}", "version=1.2"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('5ba8c08f06d546b9ac2638570ed9657e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng951bJob,
        Complete
    ]) 

    # authCheck >> csng951bJob >> Complete
    workflow








