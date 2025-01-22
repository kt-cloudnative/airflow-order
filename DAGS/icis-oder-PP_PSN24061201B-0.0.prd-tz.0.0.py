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
                , WORKFLOW_NAME='PP_PSN24061201B',WORKFLOW_ID='7e7d99fc684f4b6abbc29da78e32d399', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_PSN24061201B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 6, 14, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7e7d99fc684f4b6abbc29da78e32d399')

    psn24061201bJob_1_vol = []
    psn24061201bJob_1_volMnt = []
    psn24061201bJob_1_vol.append(getVolume('shared-volume','shared-volume'))
    psn24061201bJob_1_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    psn24061201bJob_1_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    psn24061201bJob_1_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    psn24061201bJob_1_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    psn24061201bJob_1 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '155780d7df9a4f998da3bd5e67c0476b',
        'volumes': psn24061201bJob_1_vol,
        'volume_mounts': psn24061201bJob_1_volMnt,
        'env_from':psn24061201bJob_1_env,
        'task_id':'psn24061201bJob_1',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.136',
        'arguments':["--job.name=psn24061201bJob", "wrkDivCd=1"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    psn24061201bJob_2_vol = []
    psn24061201bJob_2_volMnt = []
    psn24061201bJob_2_vol.append(getVolume('shared-volume','shared-volume'))
    psn24061201bJob_2_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    psn24061201bJob_2_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    psn24061201bJob_2_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    psn24061201bJob_2_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    psn24061201bJob_2 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd7e53b8524364b80a3c9be0fc6cab834',
        'volumes': psn24061201bJob_2_vol,
        'volume_mounts': psn24061201bJob_2_volMnt,
        'env_from':psn24061201bJob_2_env,
        'task_id':'psn24061201bJob_2',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.136',
        'arguments':["--job.name=psn24061201bJob", "wrkDivCd=2"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    psn24061201bJob_3_vol = []
    psn24061201bJob_3_volMnt = []
    psn24061201bJob_3_vol.append(getVolume('shared-volume','shared-volume'))
    psn24061201bJob_3_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    psn24061201bJob_3_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    psn24061201bJob_3_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    psn24061201bJob_3_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    psn24061201bJob_3 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '0a21e1ef8ac343fca4665e8c6b37d144',
        'volumes': psn24061201bJob_3_vol,
        'volume_mounts': psn24061201bJob_3_volMnt,
        'env_from':psn24061201bJob_3_env,
        'task_id':'psn24061201bJob_3',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.136',
        'arguments':["--job.name=psn24061201bJob", "wrkDivCd=3"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7e7d99fc684f4b6abbc29da78e32d399')

    workflow = COMMON.getICISPipeline([
        authCheck,
        psn24061201bJob_1,
        psn24061201bJob_2,
        psn24061201bJob_3,
        Complete
    ]) 

    # authCheck >> psn24061201bJob_1

psn24061201bJob_2

psn24061201bJob_3 >> Complete
    workflow








